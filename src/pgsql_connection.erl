-module(pgsql_connection).
-export([
    start_link/2,
    start_link/3,
    stop/1
]).

-export([
    prepare/4,
    unprepare/3,
    execute/3,
    execute/4,
    transaction/3,
    reset/1
]).


-behaviour(gen_statem).
-export([
    init/1,
    callback_mode/0,
    terminate/3,
    code_change/4,
    format_status/2,
    handle_event/4
]).


-include("./pgsql_protocol_messages.hrl").
-define(idle_timeout, 5000).
-define(identifier_prefix, ("erlang_pgsql_" ?MODULE_STRING)).
-define(savepoint(Id), [?identifier_prefix, "_savepoint_", integer_to_list(Id)]).
-define(record_to_map(Tag, Value), maps:from_list(
    lists:zip(record_info(fields, Tag), tl(tuple_to_list(Data))))).

-record(options, {
    transport :: transport_options(),
    database :: database_options(),
    connection :: connection_options()}).

-record(statement, {
    name :: binary(),
    parameters :: [pgsql_types:oid()],
    fields :: [#msg_row_description_field{}]
}).

%% Data records
-record(disconnected, {
    options :: #options{},
    backoff :: backoff:backoff() | undefined,
    backoff_timer = undefined :: reference() | undefined
}).

-record(connected, {
    options :: #options{},
    transport :: pgsql_transport:transport(),
    transport_tags :: pgsql_transport:tags(),
    backend_key :: {pos_integer(), pos_integer()} | undefined,
    parameters = #{} :: map(),
    codec :: pgsql_codec:codec() | undefined,
    transaction = 0 :: non_neg_integer(),
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

-record(executing_simple, {
    from :: gen_statem:from()
}).

-record(updating_types, {
    types :: pgsql_types:types()
}).

-type connection() :: gen_db_client_connection:connection().
-type statement() :: iodata().
-type prepared_statement() :: #statement{}.

-spec start_link(transport_options(), database_options()) -> {ok, connection()}.
start_link(TransportOpts, DatabaseOpts) ->
    start_link(TransportOpts, DatabaseOpts, #{}).

-spec start_link(transport_options(), database_options(), connection_options()) -> {ok, connection()}.
-type transport_options() :: #{
    host => string(),
    port => inet:port_number(),
    ssl => disable | prefer | require,
    ssl_options => [ssl:ssl_option()],
    connect_timeout => timeout(),
    reconnect => false | {backoff, Min :: non_neg_integer(), Max :: pos_integer()}
}.
-type database_options() :: #{
    user => string(),
    password => string(),
    database => string(),
    application_name => string()
}.
-type connection_options() :: #{
    codecs => [module()],
    codecs_options => map()
}.
start_link(TransportOpts, DatabaseOpts, Opts) ->
    User = maps:get(user, DatabaseOpts, "postgres"),
    TransportOpts1 = maps:merge(#{
        host => "localhost",
        port => 5432,
        ssl => prefer,
        ssl_options => [],
        connect_timeout => 15000,
        reconnect => {backoff, 200, 15000}
    }, TransportOpts),
    DatabaseOpts1 = maps:merge(#{
        user => User,
        password => "",
        database => User,
        application_name => "erlang-pgsql"
    }, DatabaseOpts),
    ConnectionOpts = maps:merge(#{
        codecs => [],
        codecs_options => #{}
    }, Opts),
    gen_statem:start_link(?MODULE, {TransportOpts1, DatabaseOpts1, ConnectionOpts}, []).

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

-spec execute(Conn, Statement, Params) -> Result | {error, term()} when
    Conn :: connection(),
    Statement :: statement() | prepared_statement(),
    Params :: [any()],
    Result :: #{command := atom(), columns := [binary()], rows := [tuple()], rows_count => non_neg_integer()}.
execute(Conn, Statement, Params) ->
    execute(Conn, Statement, Params, #{}).

-spec execute(Conn, Statement, Params, Opts) -> Result | {error, term()} when
    Conn :: connection(),
    Statement :: statement() | prepared_statement(),
    Params :: [any()],
    Opts :: #{timeout => timeout()},
    Result :: #{command := atom(), columns := [binary()], rows := [tuple()], rows_count => non_neg_integer()}.
execute(Conn, Statement, Params, Opts) ->
    case gen_statem:call(Conn, {execute, Statement, Params, Opts}) of
        {ok, C, Ref, FieldNames, FieldTypes, Codec} ->
            Fold = fun (Row, Rows) ->
                [decode_row(FieldTypes, Row, Codec) | Rows]
            end,
            Mref = start_monitor(C),
            try execute_fold(Fold, [], Ref, Mref) of
                {ok, Command, undefined, Rows} ->
                    #{command => Command, columns => FieldNames, rows => Rows};
                {ok, Command, Count, Rows} ->
                    #{command => Command, columns => FieldNames, rows => Rows, rows_count => Count};
                {error, _} = Error ->
                    Error
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
        {ReqRef, error, Reason} ->
            {error, Reason};
        {ReqRef, done, Tag} ->
            {Command, Count} = decode_tag(Tag),
            {ok, Command, Count, lists:reverse(Acc)};
        {'DOWN', Mref, _, _, Reason} ->
            exit(Reason)
    end.

-spec transaction(Conn, Fun, Opts) -> Result | {error, term()} when
    Conn :: connection(),
    Fun :: fun(() -> Result),
    Opts :: map().
transaction(Conn, Fun, Opts) ->
    case gen_statem:call(Conn, {transaction, Opts}) of
        ok ->
            try
                Result = Fun(),
                case gen_statem:call(Conn, {commit, Opts}) of
                    ok -> Result;
                    {error, _} = CommitError -> throw(CommitError)
                end
            catch
                throw:Error ->
                    ok = gen_statem:call(Conn, {rollback, Opts}),
                    Error;
                Class:Error ->
                    Stacktrace = erlang:get_stacktrace(),
                    try
                        gen_statem:call(Conn, {rollback, Opts})
                    after
                        erlang:raise(Class, Error, Stacktrace)
                    end
            end;
        {error, _} = Error ->
            Error
    end.

reset(Conn) ->
    gen_statem:cast(Conn, reset).

init({TransportOpts, DatabaseOpts, ConnectionOpts}) ->
    Options = #options{
        transport = TransportOpts,
        database = DatabaseOpts,
        connection = ConnectionOpts
    },
    {ok, disconnected, #disconnected{options = Options, backoff = backoff(TransportOpts)},
        {next_event, internal, connect}}.

callback_mode() ->
    handle_event_function.

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


handle_event(info, {DataTag, Source, Data}, _, #connected{transport_tags = {DataTag, _, _, Source}} = Connected) ->
    #connected{transport = Transport, buffer = Buffer} = Connected,
    {Messages, Rest} = pgsql_protocol:decode_messages(<<Buffer/binary, Data/binary>>),
    ok = pgsql_transport:set_opts(Transport, [{active, once}]),
    {keep_state, Connected#connected{buffer = Rest}, [{next_event, internal, Message} || Message <- Messages]};
handle_event(info, {ClosedTag, Source}, _, #connected{transport_tags = {_, ClosedTag, _, Source}} = Connected) ->
    handle_disconnect(ClosedTag, Connected);
handle_event(info, {ErrorTag, Source, Reason}, _, #connected{transport_tags = {_, _, ErrorTag, Source}} = Connected) ->
    handle_disconnect({ErrorTag, Reason}, Connected);
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
handle_event(EventType, EventContent, #executing_simple{} = Executing, Data) ->
    executing_simple(EventType, EventContent, Executing, Data);
handle_event(EventType, EventContent, #updating_types{} = UpdatingTypes, Data) ->
    updating_types(EventType, EventContent, UpdatingTypes, Data);
handle_event(EventType, EventContent, syncing, Data) ->
    syncing(EventType, EventContent, Data).

%% States


%%%% Disconnected

disconnected(internal, connect, Data) ->
    #disconnected{options = Options, backoff = Backoff} = Data,
    #options{transport = TransportOpts, database = DatabaseOpts} = Options,

    case do_connect(TransportOpts, DatabaseOpts) of
        {ok, Transport} ->
            {next_state, authenticating, #connected{
                options = Options,
                transport = Transport,
                transport_tags = pgsql_transport:get_tags(Transport)
            }};
        {error, Error} when Backoff =:= undefined ->
            {stop, Error};
        {error, Error} ->
            {_, NewBackoff} = backoff:fail(Backoff),
            error_logger:error_msg("~s: failed to connect with error: ~p~n", [?MODULE, Error]),
            {keep_state, Data#disconnected{
                backoff = NewBackoff,
                backoff_timer = backoff:fire(Backoff)
            }}
    end;

disconnected(info, {timeout, Ref, reconnect}, #disconnected{backoff_timer = Ref} = Data) ->
    {keep_state, Data#disconnected{backoff_timer = undefined}, {next_event, internal, connect}};

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

%%%% Authenticating

authenticating(internal, #msg_auth{type = ok}, Data) ->
    {next_state, configuring, Data};

authenticating(internal, #msg_auth{type = Type, data = AuthData}, Data) ->
    #connected{options = Options, transport = Transport} = Data,
    #options{database = #{user := User, password := Password}} = Options,

    case send_authenticate_reply(Transport, Type, AuthData, User, Password) of
        ok -> {keep_state, Data};
        {error, Reason} -> {stop, Reason}
    end;

authenticating(internal, #msg_error_response{fields = Fields}, Data) ->
    handle_disconnect(convert_error(Fields), Data);

authenticating(EventType, EventContent, Data) ->
    handle_event(EventType, EventContent, Data).


send_authenticate_reply(Transport, cleartext, _, _, Password) ->
    send(Transport, [#msg_password{password = Password}]);
send_authenticate_reply(Transport, md5, Salt, User, Password) ->
    % concat('md5', md5(concat(md5(concat(password, username)), random-salt)))
    MD5 = md5_hex([Password, User]),
    SaltedMD5 = md5_hex([MD5, Salt]),
    send(Transport, [#msg_password{password = ["md5", SaltedMD5]}]);
send_authenticate_reply(_Transport, Other, _Salt, _User, _Password) ->
    {error, {not_implemented, {auth, Other}}}.


md5_hex(Data) ->
    <<Int:128/unsigned>> = crypto:hash(md5, Data),
    io_lib:format("~32.16.0b", [Int]).

%%%% Configuring

configuring(internal, #msg_ready_for_query{}, Data) ->
    #connected{options = Options} = Data,
    #options{connection = #{codecs := Codecs, codecs_options := CodecsOptions}} = Options,

    {next_state, ready, Data#connected{
        codec = pgsql_codec:new(Data#connected.parameters, CodecsOptions, Codecs)
    }, [{timeout, ?idle_timeout, idle}]};

configuring(internal, #msg_backend_key_data{id = Id, secret = Secret}, Data) ->
    {keep_state, Data#connected{backend_key = {Id, Secret}}};

configuring(internal, #msg_parameter_status{name = Name, value = Value}, Data) ->
    {keep_state, Data#connected{parameters = maps:put(Name, Value, Data#connected.parameters)}};

configuring(internal, #msg_notice_response{}, Data) ->
    %% TODO: log notices?
    {keep_state, Data};

configuring(internal, #msg_error_response{fields = Fields}, Data) ->
    handle_disconnect(convert_error(Fields), Data);

configuring(EventType, EventContent, Data) ->
    handle_event(EventType, EventContent, Data).


%%%% Ready

ready({call, From}, {prepare, Name, Statement, _Opts}, Data) ->
    ok = send_prepare(Data#connected.transport, Name, Statement),
    {next_state, #preparing{from = From, name = Name}, Data};

ready(cast, {unprepare, Name, _Opts}, Data) ->
    ok = send_unprepare(Data#connected.transport, Name),
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
                    send_execute(Transport, Name, encode_params(ParamTypes, Params, Codec)),
                    {next_state,
                        #executing{client = Client, monitor = Monitor, timeout = Timeout},
                        Data,
                        {reply, From, {ok, self(), Monitor, FieldsNames, FieldsTypes, Codec}}};
                false ->
                    ok = send_sync_types(Transport),
                    {next_state, #updating_types{types = pgsql_types:new()}, Data, [postpone]}
            end;
        false ->
            {keep_state, Data, {reply, From, {error, invalid_params_length}}, {timeout, ?idle_timeout, idle}}
    end;
ready({call, From}, {execute, Statement, Params, Opts}, Data) ->
    #connected{transport = Transport} = Data,
    ok = send_prepare(Transport, <<>>, Statement),
    {next_state, #preparing{from = From, name = <<>>, execute = {Params, Opts}}, Data};

ready({call, From}, {transaction, _Opts}, Data) ->
    #connected{transport = Transport, transaction = Transaction} = Data,
    ok = send_execute_simple(Transport, if
        Transaction =:= 0 -> "start transaction";
        Transaction > 0 -> ["savepoint ", ?savepoint(Transaction)]
    end),
    {next_state, #executing_simple{from = From}, Data};

ready({call, From}, {commit, _Opts}, Data) ->
    #connected{transport = Transport, transaction = Transaction} = Data,
    case Transaction of
        0 ->
            {keep_state, Data, [{reply, From, {error, not_in_transaction}}, {timeout, ?idle_timeout, idle}]};
        N when N > 0 ->
            ok = send_execute_simple(Transport, if
                Transaction =:= 1 -> "commit";
                Transaction > 1 -> ["release savepoint ", ?savepoint(Transaction - 1)]
            end),
            {next_state, #executing_simple{from = From}, Data}
    end;

ready({call, From}, {rollback, _Opts}, Data) ->
    #connected{transport = Transport, transaction = Transaction} = Data,
    case Transaction of
        0 ->
            {keep_state, Data, [{reply, From, {error, not_in_transaction}}, {timeout, ?idle_timeout, idle}]};
        N when N > 0 ->
            ok = send_execute_simple(Transport, if
                Transaction =:= 1 -> "rollback";
                Transaction > 1 -> ["rollback to savepoint ", ?savepoint(Transaction - 1)]
            end),
            {next_state, #executing_simple{from = From}, Data}
    end;

ready(cast, reset, Data) ->
    #connected{transport = Transport, transaction = Transaction} = Data,
    case Transaction of
        0 ->
            {keep_state, Data, {timeout, ?idle_timeout, idle}};
        T when T > 0 ->
            ok = send_execute_simple(Transport, "rollback"),
            {next_state, #executing_simple{}, Data#connected{transaction = 0}}
    end;

ready({call, From}, Request, Data) ->
    {keep_state, Data, {reply, From, {error, {unhandled_request, Request}}}};

ready(timeout, idle, Data) ->
    {keep_state, Data, hibernate};

ready(EventType, EventContent, Data) ->
    handle_event(EventType, EventContent, Data).


send_prepare(Transport, Name, Statement) ->
    send(Transport, [
        #msg_parse{name = Name, statement = Statement},
        #msg_describe{type = statement, name = Name},
        #msg_sync{}
    ]).

send_unprepare(Transport, Name) ->
    send(Transport, [
        #msg_close{type = statement, name = Name},
        #msg_sync{}
    ]).

send_sync_types(Transport) ->
    StatementName = [?identifier_prefix, "_fetch_types"],
    {Statement, Results} = pgsql_types:get_query(),
    send(Transport, [
        #msg_parse{name = StatementName, statement = Statement},
        #msg_bind{portal = "", statement = StatementName, results = Results},
        #msg_close{type = statement, name = StatementName},
        #msg_execute{portal = ""},
        #msg_sync{}
    ]).

send_execute(Transport, Name, Params) ->
    send(Transport, [
        #msg_bind{
            statement = Name,
            portal = "",
            parameters = Params,
            results = [binary]
        },
        #msg_execute{portal = ""},
        #msg_sync{}
    ]).

%% Used for executing simple statements without any parameters nor rows
send_execute_simple(Transport, Statement) ->
    send(Transport, [
        #msg_parse{name = "", statement = Statement},
        #msg_bind{portal = "", statement = ""},
        #msg_execute{portal = ""},
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
    {next_state, syncing, Data, {reply, From, {error, convert_error(Fields)}}};

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

updating_types(internal, #msg_data_row{values = Row}, Updating, Data) ->
    #updating_types{types = Types} = Updating,
    {next_state, Updating#updating_types{
        types = pgsql_types:add(Row, Types)
    }, Data};

updating_types(internal, #msg_command_complete{}, Updating, Data) ->
    #updating_types{types = Types} = Updating,
    {next_state, syncing, Data#connected{
        codec = pgsql_codec:update_types(Types, Data#connected.codec)
    }};

updating_types(internal, #msg_error_response{fields = Fields}, _, Data) ->
    handle_disconnect(convert_error(Fields), Data);

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
    Client ! {Mref, error, convert_error(Fields)},
    {next_state, syncing, Data};

executing(info, {timeout, Tref, cancel_request}, #executing{timeout = Tref} = Executing, Data) ->
    #connected{transport = Transport, backend_key = BackendKey} = Data,
    #executing{monitor = Mref} = Executing,
    cancel_monitor(Mref),
    _ = send_cancel(Transport, BackendKey),
    {next_state, Executing, Data};

executing(info, {'DOWN', Mref, _, _, _}, #executing{monitor = Mref} = Executing, Data) ->
    #connected{transport = Transport, backend_key = BackendKey} = Data,
    #executing{timeout = Tref} = Executing,
    cancel_timeout(Tref),
    send_cancel(Transport, BackendKey),
    {next_state, syncing, Data};

executing(EventType, EventContent, _, Data) ->
    handle_event(EventType, EventContent, Data).


send_cancel(Transport, {Id, Secret}) ->
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


%%%% executing simple

executing_simple(internal, #msg_parse_complete{}, _State, Data) ->
    {keep_state, Data};

executing_simple(internal, #msg_bind_complete{}, _State, Data) ->
    {keep_state, Data};

executing_simple(internal, #msg_no_data{}, _State, Data) ->
    {keep_state, Data};

executing_simple(internal, #msg_command_complete{tag = Tag}, State, Data) ->
    #executing_simple{from = From} = State,
    NewTransaction = case Tag of
        <<"COMMIT">> -> 0;
        <<"ROLLBACK">> -> max(0, Data#connected.transaction - 1); %% needed because of the reset/1 implementation
        <<"BEGIN">> -> 1;
        <<"START TRANSACTION">> -> 1;
        <<"SAVEPOINT">> -> Data#connected.transaction + 1;
        <<"RELEASE">> -> Data#connected.transaction - 1;
        <<"PREPARE TRANSACTION">> -> 0
    end,
    {next_state, syncing, Data#connected{transaction = NewTransaction}, case From of
        undefined -> [];
        _ -> {reply, From, ok}
    end};

executing_simple(internal, #msg_error_response{fields = Fields}, State, Data) ->
    #executing_simple{from = From} = State,
    {next_state, syncing, Data, {reply, From, {error, convert_error(Fields)}}};

executing_simple(EventType, EventContent, _, Data) ->
    handle_event(EventType, EventContent, Data).


%%%% syncing

syncing(internal, #msg_ready_for_query{}, Data) ->
    {next_state, ready, Data, {timeout, ?idle_timeout, idle}};

syncing(EventType, EventContent, Data) ->
    handle_event(EventType, EventContent, Data).


%% Extra handlers

handle_event({call, _}, _, Data) ->
    {keep_state, Data, postpone};
handle_event(cast, _, Data) ->
    {keep_state, Data, postpone};
handle_event(_, _, Data) ->
    {keep_state, Data}.

handle_disconnect(Error, #connected{options = Options, transport = Transport}) ->
    #options{transport = TransportOpts} = Options,
    _ = pgsql_transport:close(Transport),
    case backoff(TransportOpts) of
        undefined ->
            {stop, Error};
        Backoff ->
            error_logger:error_msg("~s: an error occured: ~p~n", [?MODULE, Error]),
            {next_state, disconnected, #disconnected{
                options = Options,
                backoff = Backoff,
                backoff_timer = backoff:fire(Backoff)
            }}
    end.


%% Encode/Decode

encode_params(Types, Values, Codec) ->
    lists:zipwith(fun
        (_Type, null) -> {binary, null};
        (Type, Value) -> {binary, pgsql_codec:encode(Type, Value, Codec)}
    end, Types, Values).

decode_row(Types, Values, Codec) ->
    list_to_tuple(lists:zipwith(fun
        (_Type, null) -> null;
        (Type, Value) -> pgsql_codec:decode(Type, Value, Codec)
    end, Types, Values)).

decode_tag(<<"SELECT ", Count/binary>>) ->
    {select, binary_to_integer(Count)};
decode_tag(<<"INSERT ", Rest/binary>>) ->
    [_, Count] = binary:split(Rest, <<" ">>),
    {insert, binary_to_integer(Count)};
decode_tag(<<"UPDATE ", Count/binary>>) ->
    {update, binary_to_integer(Count)};
decode_tag(<<"DELETE ", Count/binary>>) ->
    {delete, binary_to_integer(Count)};
decode_tag(<<"FETCH ", Count/binary>>) ->
    {fetch, binary_to_integer(Count)};
decode_tag(<<"MOVE ", Count/binary>>) ->
    {move, binary_to_integer(Count)};
decode_tag(<<"COPY ", Count/binary>>) ->
    {copy, binary_to_integer(Count)};
decode_tag(Other) ->
    decode_tag(Other, <<>>).

decode_tag(<<>>, Acc) ->
    {binary_to_atom(Acc, utf8), undefined};
decode_tag(<<" ", Rest/binary>>, Acc) ->
    decode_tag(Rest, <<Acc/binary, "_">>);
decode_tag(<<C, Rest/binary>>, Acc) when C >= $A, C =< $Z ->
    decode_tag(Rest, <<Acc/binary, (C + 32)>>);
decode_tag(<<C, Rest/binary>>, Acc) ->
    decode_tag(Rest, <<Acc/binary, C>>).

%% Helpers

send(Transport, Messages) ->
    pgsql_transport:send(Transport, pgsql_protocol:encode_messages(Messages)).

backoff(#{reconnect := {backoff, Min, Max}}) ->
    backoff:type(backoff:init(Min, Max, self(), reconnect), jitter);
backoff(#{reconnect := false}) ->
    undefined.

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

convert_error(#{name := query_canceled}) -> timeout;
convert_error(Other) -> Other.
