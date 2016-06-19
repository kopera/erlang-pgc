-module(pgsql_connection).
-export([
    start_link/2,
    stop/1
]).

-export([
    prepare/4,
    unprepare/3,
    execute/4
]).


-behaviour(gen_statem).
-export([
    init/1,
    terminate/3,
    code_change/4,
    format_status/2,
    handle_event/4
]).


-include("./pgsql_protocol_messages.hrl").
-define(idle_timeout, timer:seconds(5)).
-define(backoff_init, timer:seconds(1)).
-define(backoff_max, timer:seconds(30)).
-define(record_to_map(Tag, Value), maps:from_list(
    lists:zip(record_info(fields, Tag), tl(tuple_to_list(Data))))).

-record(options, {
    transport :: transport_options(),
    database :: database_options()
}).

-record(statement, {
    name :: binary(),
    parameters :: [pgsql_types:oid()],
    fields :: [#msg_row_description_field{}]
}).

%% Data records
-record(disconnected, {
    options :: #options{},
    backoff_delay = ?backoff_init :: pos_integer(),
    backoff_timer = undefined :: reference() | undefined
}).

-record(connected, {
    options :: #options{},
    transport :: pgsql_transport:transport(),
    transport_tags :: pgsql_transport:tags(),
    backend_key :: {pos_integer(), pos_integer()} | undefined,
    parameters = #{} :: map(),
    codec :: pgsql_codec:codec() | undefined,
    buffer = <<>> :: binary()
}).

%% State records
-record(preparing, {
    from :: gen_statem:from(),
    name :: binary(),
    parameters = [] :: [pgsql_types:oid()],
    execute = false :: false | {[Parameters :: any()], map()}
}).

-record(executing, {
    client :: pid(),
    monitor :: reference(),
    timeout :: reference() | undefined
}).

-record(updating_types, {
    types :: pgsql_types:types()
}).

-type connection() :: gen_db_client_connection:connection().
-type statement() :: iodata().
-type prepared_statement() :: #statement{}.


-spec start_link(transport_options(), database_options()) -> {ok, connection()}.
-type transport_options() :: #{
    host => string(),
    port => inet:port_number(),
    ssl => disable | prefer | require,
    ssl_options => [ssl:ssl_option()],
    connect_timeout => timeout()
}.
-type database_options() :: #{
    user => string(),
    password => string(),
    database => string(),
    application_name => string()
}.
start_link(TransportOpts, DatabaseOpts) ->
    gen_statem:start_link(?MODULE, {TransportOpts, DatabaseOpts}, []).

-spec stop(Conn) -> ok when Conn :: connection().
stop(Conn) ->
    gen_statem:stop(Conn).

-spec prepare(Conn, Name, Statement, Opts) -> {ok, PreparedStatement} | {error, term()} when
    Conn :: connection(),
    Name :: iodata(),
    Statement :: statement(),
    Opts :: map(),
    PreparedStatement :: prepared_statement().
prepare(Conn, Name, Statement, Opts) ->
    gen_statem:call(Conn, {prepare, unicode:characters_to_binary(Name), Statement, Opts}).

-spec unprepare(Conn, PreparedStatement, Opts) -> ok when
    Conn :: connection(),
    PreparedStatement :: prepared_statement(),
    Opts :: map().
unprepare(Conn, #statement{name = Name}, Opts) ->
    gen_statem:cast(Conn, {unprepare, Name, Opts}).

-spec execute(Conn, Statement, Params, Opts) -> {ok, Fields, Rows} | {error, term()} when
    Conn :: connection(),
    Statement :: statement() | prepared_statement(),
    Params :: [any()],
    Opts :: map(),
    Fields :: [binary()],
    Rows :: [Row],
    Row :: [any()].
execute(Conn, Statement, Params, Opts) ->
    case gen_statem:call(Conn, {execute, Statement, Params, Opts}) of
        {ok, C, Ref, FieldNames, FieldTypes, Codec} ->
            Fold = fun (Row, Rows) ->
                [do_decode_row(FieldTypes, Row, Codec) | Rows]
            end,
            Mref = start_monitor(C),
            try execute_fold(Fold, [], Ref, Mref) of
                {ok, Rows} -> {ok, FieldNames, lists:reverse(Rows)};
                {error, _} = Error -> Error
            after
                cancel_monitor(Mref)
            end;
        {error, _} = Error ->
            Error
    end.

execute_fold(Fun, Acc, ReqRef, Mref) ->
    receive
        {ReqRef, row, Row} ->
            execute_fold(Fun, Fun(Row, Acc), ReqRef, Mref);
        {ReqRef, fields, Fields} ->
            execute_fold(Fun, Fun(fields, Fields, Acc), ReqRef, Mref);
        {ReqRef, error, Reason} ->
            {error, Reason};
        {ReqRef, done, _Tag} ->
            {ok, Acc};
        {'DOWN', Mref, _, _, Reason} ->
            exit(Reason)
    end.

do_decode_row(Types, Values, Codec) ->
    list_to_tuple(lists:zipwith(fun
        (_Type, null) -> null;
        (Type, Value) -> pgsql_codec:decode(Type, Value, Codec)
    end, Types, Values)).


init({TransportOpts, DatabaseOpts}) ->
    User = maps:get(user, DatabaseOpts, "postgres"),
    Options = #options{
        transport = maps:merge(#{
            host => "localhost",
            port => 5432,
            ssl => prefer,
            ssl_options => [],
            connect_timeout => 5000
        }, TransportOpts),
        database = maps:merge(#{
            user => User,
            password => "",
            database => User,
            application_name => "erlang-pgsql"
        }, DatabaseOpts)
    },
    {handle_event_function, disconnected, #disconnected{options = Options}, {next_event, internal, connect}}.

terminate(_Reason, _State, _Data) ->
    ok.

code_change(_OldVsn, _OldState, _OldData, _Extra) ->
    erlang:exit(not_implemented).

format_status(terminate, [_PDict, State, Data]) ->
    {State, format_status_data(Data)};
format_status(_, [_PDict, State, Data]) ->
    [{data, [{"State", {State, format_status_data(Data)}}]}].

format_status_data(#connected{} = Data) ->
    maps:without([transport_tags, codec, buffer], ?record_to_map(connected, Data));
format_status_data(#disconnected{} = Data) ->
    ?record_to_map(disconnected, Data).


handle_event(info, {Tag, Source, Data}, _, #connected{transport_tags = {Tag, _, _, Source}} = Connected) ->
    #connected{transport = Transport, buffer = Buffer} = Connected,
    {Messages, Rest} = pgsql_protocol:decode_messages(<<Buffer/binary, Data/binary>>),
    ok = pgsql_transport:set_opts(Transport, [{active, once}]),
    {keep_state, Connected#connected{buffer = Rest}, [{next_event, internal, Message} || Message <- Messages]};
handle_event(info, {Tag, Source}, _, #connected{transport_tags = {_, Closed, Error, Source}} = Connected)
        when Tag =:= Closed; Tag =:= Error ->
    #connected{options = Options, transport = Transport} = Connected,
    _ = pgsql_transport:close(Transport),
    {next_state, disconnected, #disconnected{options = Options}, {next_event, internal, connect}};
handle_event(EventType, EventContent, disconnected, Data) ->
    disconnected(EventType, EventContent, Data);
handle_event(EventType, EventContent, authenticating, Data) ->
    authenticating(EventType, EventContent, Data);
handle_event(EventType, EventContent, configuring, Data) ->
    configuring(EventType, EventContent, Data);
handle_event(EventType, EventContent, ready, Data) ->
    ready(EventType, EventContent, Data);
handle_event(EventType, EventContent, #preparing{} = Preparing, Data) ->
    preparing(EventType, EventContent, Preparing, Data);
handle_event(EventType, EventContent, #executing{} = Executing, Data) ->
    executing(EventType, EventContent, Executing, Data);
handle_event(EventType, EventContent, #updating_types{} = UpdatingTypes, Data) ->
    updating_types(EventType, EventContent, UpdatingTypes, Data);
handle_event(EventType, EventContent, syncing, Data) ->
    syncing(EventType, EventContent, Data).

%% States


%%%% Disconnected

disconnected(internal, connect, Data) ->
    #disconnected{options = Options} = Data,
    #options{transport = TransportOpts, database = DatabaseOpts} = Options,

    case do_connect(TransportOpts, DatabaseOpts) of
        {ok, Transport} ->
            {next_state, authenticating, #connected{
                options = Options,
                transport = Transport,
                transport_tags = pgsql_transport:get_tags(Transport)
            }};
        {error, Error} ->
            error_logger:error_msg("~s: failed to connect with error: ~p~n", [?MODULE, Error]),
            {keep_state, do_backoff(Data)}
    end;

disconnected(info, {timeout, Ref, reconnect}, #disconnected{backoff_timer = Ref} = Data) ->
    {keep_state, Data, {next_event, internal, connect}};

disconnected({call, From}, _, Data) ->
    {keep_state, Data, {reply, From, {error, disconnected}}};

disconnected(EventType, EventContent, Data) ->
    handle_event(EventType, EventContent, Data).

do_connect(TransportOpts, DatabaseOpts) ->
    #{
        host := Host,
        port := Port,
        ssl := SSL,
        ssl_options := SSLOpts,
        connect_timeout := ConnectTimeout
    } = TransportOpts,
    case pgsql_transport:open(Host, Port, SSL, SSLOpts, ConnectTimeout) of
        {ok, Transport} ->
            ok = send(Transport, [#msg_startup{
                parameters = maps:without([password], DatabaseOpts)
            }]),
            ok = pgsql_transport:set_opts(Transport, [{active, once}]),
            {ok, Transport};
        {error, _} = Error ->
            Error
    end.

do_backoff(#disconnected{backoff_delay = BackoffDelay} = Data) ->
    Data#disconnected{
        backoff_delay = backoff:rand_increment(BackoffDelay, ?backoff_max),
        backoff_timer = reconnect_timout(BackoffDelay)
    }.

%%%% Authenticating

authenticating(internal, #msg_auth{type = ok}, Data) ->
    {next_state, configuring, Data};

authenticating(internal, #msg_auth{type = Type, data = AuthData}, Data) ->
    #connected{options = Options, transport = Transport} = Data,
    #options{database = #{user := User, password := Password}} = Options,

    case do_authenticate(Transport, Type, AuthData, User, Password) of
        ok -> {keep_state, Data};
        {error, Reason} -> {stop, Reason}
    end;

authenticating(internal, #msg_error_response{fields = Fields}, Data) ->
    handle_fatal_error(Fields, Data);

authenticating(EventType, EventContent, Data) ->
    handle_event(EventType, EventContent, Data).

do_authenticate(Transport, cleartext, _, _, Password) ->
    send(Transport, [#msg_password{password = Password}]);
do_authenticate(Transport, md5, Salt, User, Password) ->
    % concat('md5', md5(concat(md5(concat(password, username)), random-salt)))
    MD5 = io_lib:format("~32.16.0b", [crypto:hash(md5, [Password, User])]),
    SaltedMD5 = io_lib:format("~32.16.0b", [crypto:hash(md5, [MD5, Salt])]),
    send(Transport, [#msg_password{password = ["md5", SaltedMD5]}]);
do_authenticate(_Transport, Other, _Salt, _User, _Password) ->
    {error, {not_implemented, {auth, Other}}}.


%%%% Configuring

configuring(internal, #msg_ready_for_query{}, Data) ->
    {next_state, ready, Data#connected{
        codec = pgsql_codec:new(Data#connected.parameters, #{})
    }, [{timeout, ?idle_timeout, idle}]};

configuring(internal, #msg_backend_key_data{id = Id, secret = Secret}, Data) ->
    {keep_state, Data#connected{backend_key = {Id, Secret}}};

configuring(internal, #msg_parameter_status{name = Name, value = Value}, Data) ->
    {keep_state, Data#connected{parameters = maps:put(Name, Value, Data#connected.parameters)}};

configuring(internal, #msg_notice_response{}, Data) ->
    %% TODO: log notices?
    {keep_state, Data};

configuring(internal, #msg_error_response{fields = Fields}, Data) ->
    handle_fatal_error(Fields, Data);

configuring(EventType, EventContent, Data) ->
    handle_event(EventType, EventContent, Data).


%%%% Ready

ready({call, From}, {prepare, Name, Statement, _Opts}, Data) ->
    ok = do_prepare(Data#connected.transport, Name, Statement),
    {next_state, #preparing{from = From, name = Name}, Data};

ready(cast, {unprepare, Name, _Opts}, Data) ->
    ok = do_unprepare(Data#connected.transport, Name),
    {next_state, syncing, Data};

ready({call, {Client, _} = From}, {execute, #statement{} = Statement, Params, Opts}, Data) ->
    #statement{name = Name, parameters = ParamTypes, fields = FieldsDesc} = Statement,
    #connected{transport = Transport, codec = Codec} = Data,
    case length(ParamTypes) =:= length(Params) of
        true ->
            FieldsNames = [FieldName || #msg_row_description_field{name = FieldName} <- FieldsDesc],
            FieldsTypes = [FieldType || #msg_row_description_field{type_oid = FieldType} <- FieldsDesc],
            case pgsql_codec:has_types(ParamTypes ++ FieldsTypes, Codec) of
                true ->
                    Monitor = start_monitor(Client),
                    Timeout = request_timeout(maps:get(timeout, Opts, infinity)),
                    do_execute(Transport, Name, do_encode_params(ParamTypes, Params, Codec)),
                    {next_state,
                        #executing{client = Client, monitor = Monitor, timeout = Timeout},
                        Data,
                        {reply, From, {ok, self(), Monitor, FieldsNames, FieldsTypes, Codec}}};
                false ->
                    ok = do_update_types(Transport),
                    {next_state, #updating_types{types = pgsql_types:new()}, Data, [postpone]}
            end;
        false ->
            {keep_state, Data, {reply, From, {error, invalid_params_length}}}
    end;
ready({call, From}, {execute, Statement, Params, Opts}, Data) ->
    #connected{transport = Transport} = Data,
    ok = do_prepare(Transport, <<>>, Statement),
    {next_state, #preparing{from = From, name = <<>>, execute = {Params, Opts}}, Data};

ready({call, From}, Request, Data) ->
    {keep_state, Data, {reply, From, {error, {unhandled_request, Request}}}};

ready(timeout, idle, Data) ->
    {keep_state, Data, hibernate};

ready(EventType, EventContent, Data) ->
    handle_event(EventType, EventContent, Data).


do_prepare(Transport, Name, Statement) ->
    send(Transport, [
        #msg_parse{name = Name, statement = Statement},
        #msg_describe{type = statement, name = Name},
        #msg_sync{}
    ]).

do_unprepare(Transport, Name) ->
    send(Transport, [
        #msg_close{type = statement, name = Name},
        #msg_sync{}
    ]).

do_update_types(Transport) ->
    StatementName = "erlang-pgsql:" ++ ?MODULE_STRING ++ ":do_update_types/1",
    {Statement, Results} = pgsql_types:get_query(),
    send(Transport, [
        #msg_parse{name = StatementName, statement = Statement},
        #msg_bind{portal = "", statement = StatementName, results = Results},
        #msg_close{type = statement, name = StatementName},
        #msg_execute{portal = "", limit = 0},
        #msg_sync{}
    ]).

do_encode_params(Types, Values, Codec) ->
    lists:zipwith(fun
        (_Type, null) -> {binary, null};
        (Type, Value) -> {binary, pgsql_codec:encode(Type, Value, Codec)}
    end, Types, Values).

do_execute(Transport, Name, Params) ->
    send(Transport, [
        #msg_bind{
            statement = Name,
            portal = "",
            parameters = Params,
            results = [binary]
        },
        #msg_execute{portal = "", limit = 0},
        #msg_sync{}
    ]).

%%%% preparing

preparing(internal, #msg_parse_complete{}, _State, Data) ->
    {keep_state, Data};

preparing(internal, #msg_parameter_description{types = Types}, Preparing, Data) ->
    {next_state, Preparing#preparing{parameters = Types}, Data};

preparing(internal, #msg_row_description{fields = Fields}, Preparing, Data) ->
    preparing_done(Fields, Preparing, Data);

preparing(internal, #msg_no_data{}, Preparing, Data) ->
    preparing_done([], Preparing, Data);

preparing(internal, #msg_error_response{fields = Fields}, Preparing, Data) ->
    #preparing{from = From} = Preparing,
    {next_state, syncing, Data, {reply, From, {error, Fields}}};

preparing(EventType, EventContent, _, Data) ->
    handle_event(EventType, EventContent, Data).


preparing_done(Fields, #preparing{execute = false} = Preparing, Data) ->
    #preparing{from = From, name = Name, parameters = Parameters} = Preparing,
    Statement = #statement{
        name = Name,
        parameters = Parameters,
        fields = Fields
    },
    {next_state, syncing, Data, {reply, From, {ok, Statement}}};
preparing_done(Fields, #preparing{execute = {Params, Opts}} = Preparing, Data) ->
    #preparing{from = From, name = Name, parameters = Parameters} = Preparing,
    Statement = #statement{
        name = Name,
        parameters = Parameters,
        fields = Fields
    },
    {next_state, syncing, Data, {next_event, {call, From}, {execute, Statement, Params, Opts}}}.

%%%% updating_types

updating_types(internal, #msg_parse_complete{}, _, Data) ->
    {keep_state, Data};

updating_types(internal, #msg_bind_complete{}, _, Data) ->
    {keep_state, Data};

updating_types(internal, #msg_close_complete{}, _, Data) ->
    {keep_state, Data};

updating_types(internal, #msg_data_row{values = Row}, #updating_types{types = Types} = Updating, Data) ->
    {next_state, Updating#updating_types{
        types = pgsql_types:add(Row, Types)
    }, Data};

updating_types(internal, #msg_command_complete{}, #updating_types{types = Types}, Data) ->
    {next_state, syncing, Data#connected{
        codec = pgsql_codec:update_types(Types, Data#connected.codec)
    }};

updating_types(internal, #msg_error_response{fields = Fields}, _, Data) ->
    handle_fatal_error(Fields, Data);

updating_types(EventType, EventContent, _, Data) ->
    handle_event(EventType, EventContent, Data).


%%%% executing

executing(internal, #msg_bind_complete{}, _, Data) ->
    {keep_state, Data};

executing(internal, #msg_data_row{values = Values}, Executing, Data) ->
    #executing{client = Client, monitor = Ref} = Executing,
    Client ! {Ref, row, Values},
    {next_state, Executing, Data};

executing(internal, #msg_command_complete{tag = Tag}, Executing, Data) ->
    #executing{client = Client, monitor = Mref, timeout = Tref} = Executing,
    cancel_timeout(Tref),
    cancel_monitor(Mref),
    Client ! {Mref, done, Tag},
    {next_state, syncing, Data};

executing(internal, #msg_no_data{}, Executing, Data) ->
    #executing{monitor = Mref, timeout = Tref} = Executing,
    cancel_timeout(Tref),
    cancel_monitor(Mref),
    {next_state, syncing, Data};

executing(internal, #msg_empty_query_response{}, Executing, Data) ->
    #executing{client = Client, monitor = Mref, timeout = Tref} = Executing,
    cancel_timeout(Tref),
    cancel_monitor(Mref),
    Client ! {Mref, done, undefined},
    {next_state, syncing, Data};

executing(internal, #msg_error_response{fields = Fields}, Executing, Data) ->
    #executing{client = Client, monitor = Mref, timeout = Tref} = Executing,
    cancel_timeout(Tref),
    cancel_monitor(Mref),
    Client ! {Mref, error, Fields},
    {next_state, syncing, Data};

executing(info, {timeout, Tref, cancel_request}, #executing{timeout = Tref} = Executing, Data) ->
    #connected{transport = Transport, backend_key = BackendKey} = Data,
    #executing{client = Client, monitor = Mref, timeout = Tref} = Executing,
    cancel_monitor(Mref),
    Client ! {Mref, error, timeout},
    _ = do_cancel(Transport, BackendKey),
    {next_state, syncing, Data};

executing(info, {'DOWN', Mref, _, _, _}, #executing{monitor = Mref} = Executing, Data) ->
    #connected{transport = Transport, backend_key = BackendKey} = Data,
    #executing{timeout = Tref} = Executing,
    cancel_timeout(Tref),
    do_cancel(Transport, BackendKey),
    {next_state, syncing, Data};

executing(EventType, EventContent, _, Data) ->
    handle_event(EventType, EventContent, Data).


do_cancel(Transport, {Id, Secret}) ->
    case pgsql_transport:dup(Transport) of
        {ok, T} ->
            _ = send(T, [#msg_cancel_request{
                id = Id,
                secret = Secret
            }]),
            _ = pgsql_transport:recv(T, 1),
            _ = pgsql_transport:close(T),
            ok;
        {error, _} = Error ->
            Error
    end.


%%%% syncing

syncing(internal, #msg_ready_for_query{}, Data) ->
    {next_state, ready, Data, {timeout, ?idle_timeout, idle}};

syncing(EventType, EventContent, Data) ->
    handle_event(EventType, EventContent, Data).


%% Extra handlers

handle_event({call, _}, _, Data) ->
    {keep_state, Data, [postpone]};
handle_event(_, _, Data) ->
    {keep_state, Data}.

handle_fatal_error(Error, #connected{options = Options, transport = Transport}) ->
    _ = pgsql_transport:close(Transport),
    error_logger:error_msg("~s: an error occured: ~p~n", [?MODULE, Error]),
    {next_state, disconnected, do_backoff(#disconnected{options = Options})}.

%% Helpers

send(Transport, Messages) ->
    pgsql_transport:send(Transport, pgsql_protocol:encode_messages(Messages)).

reconnect_timout(BackoffDelay) ->
    erlang:start_timer(BackoffDelay, self(), reconnect).

request_timeout(infinity) ->
    undefined;
request_timeout(Timeout) ->
    erlang:start_timer(Timeout, self(), cancel_request).

cancel_timeout(undefined) ->
    ok;
cancel_timeout(Ref) ->
    erlang:cancel_timer(Ref, [{async, true}]).

start_monitor(Pid) ->
    erlang:monitor(process, Pid).

cancel_monitor(Ref) ->
    erlang:demonitor(Ref).
