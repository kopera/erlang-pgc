-module(pgsql_connection).
-export([
    start_link/2,
    stop/1
]).

-export([
    prepare/3,
    unprepare/3,
    execute/4,
    transaction/3
]).

-behaviour(gen_db_client_connection).
-export([
    connect/1,
    checkout/2,
    checkin/2,
    ping/1,
    handle_begin/2,
    handle_commit/2,
    handle_rollback/2,
    handle_prepare/3,
    handle_execute/4,
    handle_unprepare/3,
    handle_info/2,
    disconnect/2
]).

-include("./pgsql_protocol_messages.hrl").
-define(recv_timeout, 5000).

-record(state, {
    transport :: pgsql_transport:transport(),
    backend_key :: {pos_integer(), pos_integer()},
    parameters :: #{binary() => binary()},
    codec :: pgsql_codec:codec(),
    buffer = <<>> :: binary()
}).

-record(statement, {
    name = <<>> :: binary(),
    parameters = [] :: [pgsql_types:oid()],
    fields = [] :: [#msg_row_description_field{}]
}).

-type connection() :: gen_db_client_connection:connection().
-type query() :: iodata().
-type prepared_query() :: #statement{}.


-spec start_link(TransportOpts, DatabaseOpts) -> {ok, connection()} when
    TransportOpts ::  #{
        host => string(),
        port => inet:port_number(),
        ssl => disable | prefer | require,
        ssl_options => [ssl:ssl_option()],
        connect_timeout => timeout()
    },
    DatabaseOpts :: #{
        user => string(),
        password => string(),
        database => string(),
        application_name => string()
    }.
start_link(TransportOpts, DatabaseOpts) ->
    gen_db_client_connection:start_link(?MODULE, {TransportOpts, DatabaseOpts}).

-spec stop(Conn) -> ok when Conn :: connection().
stop(Conn) ->
    gen_db_client_connection:stop(Conn).

-spec prepare(Conn, Query, Opts) -> {ok, PreparedQuery} | {error, term()} when
    Conn :: connection(),
    Query :: query(),
    Opts :: map(),
    PreparedQuery :: prepared_query().
prepare(Conn, Query, Opts) ->
    gen_db_client_connection:prepare(Conn, Query, Opts).

-spec unprepare(Conn, PreparedQuery, Opts) -> ok when
    Conn :: connection(),
    PreparedQuery :: prepared_query(),
    Opts :: map().
unprepare(Conn, PreparedQuery, Opts) ->
    gen_db_client_connection:unprepare(Conn, PreparedQuery, Opts).

-spec execute(Conn, Query, Params, Opts) -> {ok, {Columns, Rows}} | {error, term()} when
    Conn :: connection(),
    Query :: query() | prepared_query(),
    Params :: [any()],
    Opts :: map(),
    Columns :: [binary()],
    Rows :: [Row],
    Row :: [any()].
execute(Conn, Query, Params, Opts) ->
    gen_db_client_connection:execute(Conn, Query, Params, Opts).

transaction(Conn, Fun, Opts) ->
    gen_db_client_connection:transaction(Conn, Fun, Opts).


%% @hidden
connect({TransportOpts, DatabaseOpts}) ->
    Host = maps:get(host, TransportOpts, "localhost"),
    Port = maps:get(port, TransportOpts, 5432),
    SSL = maps:get(ssl, TransportOpts, prefer),
    SSLOpts = maps:get(ssl_options, TransportOpts, []),
    ConnectTimeout = maps:get(connect_timeout, TransportOpts, 5000),
    case pgsql_transport:open(Host, Port, SSL, SSLOpts, ConnectTimeout) of
        {ok, Transport} ->
            User = maps:get(user, DatabaseOpts, "postgres"),
            Password = maps:get(password, DatabaseOpts, ""),
            ok = pgsql_transport:send(Transport, [#msg_startup{
                parameters = maps:without([password], maps:merge(#{database => User}, DatabaseOpts))
            }]),
            authenticate(Transport, User, Password);
        {error, _} = Error ->
            Error
    end.

authenticate(Transport, User, Password) ->
    case pgsql_transport:recv(Transport, ?recv_timeout) of
        {ok, #msg_auth{type = ok}, Transport1} ->
            configure(Transport1, undefined, #{});
        {ok, #msg_auth{type = cleartext}, Transport1} ->
            ok = pgsql_transport:send(Transport1, [#msg_password{password = Password}]),
            authenticate(Transport1, User, Password);
        {ok, #msg_auth{type = md5, data = Salt}, Transport1} ->
            % concat('md5', md5(concat(md5(concat(password, username)), random-salt)))
            MD5 = io_lib:format("~32.16.0b", [crypto:hash(md5, [Password, User])]),
            SaltedMD5 = io_lib:format("~32.16.0b", [crypto:hash(md5, [MD5, Salt])]),
            ok = pgsql_transport:send(Transport1, [#msg_password{password = ["md5", SaltedMD5]}]),
            authenticate(Transport1, User, Password);
        {ok, #msg_auth{type = Other}, _Transport1} ->
            exit({not_implemented, {auth, Other}});
        {ok, #msg_error_response{fields = Fields}, Transport1} ->
            _ = pgsql_transport:close(Transport1),
            {error, Fields};
        {error, Reason} ->
            _ = pgsql_transport:close(Transport),
            {error, Reason}
    end.

configure(Transport, BackendKey, Parameters) ->
    case pgsql_transport:recv(Transport, ?recv_timeout) of
        {ok, #msg_ready_for_query{}, Transport1} ->
            {ok, #state{
                transport = Transport1,
                backend_key = BackendKey,
                parameters = Parameters,
                codec = pgsql_codec:new(Parameters, #{})
            }};
        {ok, #msg_backend_key_data{id = Id, secret = Secret}, Transport1} ->
            configure(Transport1, {Id, Secret}, Parameters);
        {ok, #msg_parameter_status{name = Name, value = Value}, Transport1} ->
            configure(Transport1, BackendKey, maps:put(Name, Value, Parameters));
        {ok, #msg_notice_response{}, Transport1} ->
            %% TODO: log notice?
            configure(Transport1, BackendKey, Parameters);
        {ok, #msg_error_response{fields = Fields}, Transport1} ->
            _ = pgsql_transport:close(Transport1),
            {error, Fields};
        {error, Reason} ->
            _ = pgsql_transport:close(Transport),
            {error, Reason}
    end.

checkout(_User, State) ->
    {ok, State}.

checkin(_User, State) ->
    {ok, State}.

ping(State) ->
    {ok, State}.

handle_begin(_Opts, State) ->
    {error, not_implemented, State}.

handle_commit(_Opts, State) ->
    {error, not_implemented, State}.

handle_rollback(_Opts, State) ->
    {error, not_implemented, State}.

handle_prepare({Name, Statement}, _Opts, #state{transport = Transport} = State) ->
    case do_prepare(Transport, Name, Statement) of
        {done, {ok, Reply}, Transport1} ->
            {ok, Reply, State#state{transport = Transport1}};
        {done, {error, Reason}, Transport1} ->
            {error, Reason, State#state{transport = Transport1}};
        {disconnected, Reason} ->
            {disconnect, Reason}
    end.

handle_execute(#statement{name = Name, parameters = ParamTypes, fields = FieldsDesc}, Params, _Opts, State) ->
    case length(ParamTypes) =:= length(Params) of
        true ->
            FieldsNames = [FieldName || #msg_row_description_field{name = FieldName} <- FieldsDesc],
            FieldsTypes = [FieldType || #msg_row_description_field{type_oid = FieldType} <- FieldsDesc],
            {Codec, Transport} = codec_sync(State#state.transport, State#state.codec, ParamTypes ++ FieldsTypes),
            case do_execute(Transport, Name, codec_encode(ParamTypes, Params, Codec)) of
                {done, {ok, _Tag, Rows}, Transport1} ->
                    DecodedRows = [codec_decode(FieldsTypes, FieldsValues, Codec) || FieldsValues <- Rows],
                    {ok, {FieldsNames, DecodedRows}, State#state{transport = Transport1, codec = Codec}};
                {done, {error, Reason}, Transport1} ->
                    {error, Reason, State#state{transport = Transport1, codec = Codec}};
                {disconnected, Reason} ->
                    {disconnect, Reason}
            end;
        false ->
            {error, invalid_params_length, State}
    end;
handle_execute(Statement, Params, Opts, #state{transport = Transport} = State) ->
    case do_prepare(Transport, "", Statement) of
        {done, {ok, PreparedStatement}, Transport1} ->
            handle_execute(PreparedStatement, Params, Opts, State#state{transport = Transport1});
        {done, {error, Reason}, Transport1} ->
            {error, Reason, State#state{transport = Transport1}};
        {disconnected, Reason} ->
            {disconnect, Reason}
    end.

handle_unprepare(#statement{name = Name}, _Opts, #state{transport = Transport} = State) ->
    case do_unprepare(Transport, Name) of
        {done, ok, Transport1} ->
            {ok, State#state{transport = Transport1}};
        {done, {error, Reason}, Transport1} ->
            {error, Reason, State#state{transport = Transport1}};
        {disconnected, Reason} ->
            {disconnect, Reason}
    end.

handle_info(_Msg, State) ->
    {ok, State}.

disconnect(_Info, #state{transport = Transport}) ->
    _ = pgsql_transport:send(Transport, [#msg_terminate{}]),
    _ = pgsql_transport:close(Transport),
    ok.


%% Codec

codec_sync(Transport, Codec, Types) ->
    case pgsql_codec:has_types(Types, Codec) of
        true ->
            {Codec, Transport};
        false ->
            {TypeInfos, Transport1} = pgsql_types:load(Transport),
            {pgsql_codec:update_types(TypeInfos, Codec), Transport1}
    end.

codec_encode(Types, Values, Codec) ->
    lists:zipwith(fun
        (_Type, null) -> {binary, null};
        (Type, Value) -> {binary, pgsql_codec:encode(Type, Value, Codec)}
    end, Types, Values).

codec_decode(Types, Values, Codec) ->
    lists:zipwith(fun
        (_Type, null) -> null;
        (Type, Value) -> pgsql_codec:decode(Type, Value, Codec)
    end, Types, Values).


%% Prepare protocol

do_prepare(Transport, Name, Statement) ->
    Messages = [
        #msg_parse{name = Name, statement = Statement},
        #msg_describe{type = statement, name = Name},
        #msg_sync{}
    ],
    case pgsql_transport:send(Transport, Messages) of
        ok -> do_prepare_parsing(Transport, Name);
        {error, _} = Error -> {disconnect, Error}
    end.

do_prepare_parsing(Transport, Name) ->
    case pgsql_transport:recv(Transport, ?recv_timeout) of
        {ok, #msg_parse_complete{}, Transport1} ->
            do_prepare_describing_parameters(Transport1, Name);
        {ok, #msg_error_response{fields = Details}, Transport1} ->
            do_sync(Transport1, {error, Details});
        {error, _} = Error ->
            {disconnected, Error}
    end.

do_prepare_describing_parameters(Transport, Name) ->
    case pgsql_transport:recv(Transport, ?recv_timeout) of
        {ok, #msg_parameter_description{types = ParametersTypes}, Transport1} ->
            do_prepare_describing_row(Transport1, Name, ParametersTypes);
        {ok, #msg_error_response{fields = Details}, Transport1} ->
            do_sync(Transport1, {error, Details});
        {error, _} = Error ->
            {disconnected, Error}
    end.

do_prepare_describing_row(Transport, Name, ParametersTypes) ->
    case pgsql_transport:recv(Transport, ?recv_timeout) of
        {ok, #msg_row_description{fields = Fields}, Transport1} ->
            do_sync(Transport1, {ok, #statement{name = Name, parameters = ParametersTypes, fields = Fields}});
        {ok, #msg_no_data{}, Transport1} ->
            do_sync(Transport1, {ok, #statement{name = Name, parameters = ParametersTypes}});
        {error, _} = Error ->
            {disconnected, Error}
    end.

%% Unprepare protocol

do_unprepare(Transport, Name) ->
    Messages = [
        #msg_close{type = statement, name = Name},
        #msg_sync{}
    ],
    case pgsql_transport:send(Transport, Messages) of
        ok -> do_unprepare_closing(Transport);
        {error, _} = Error -> {disconnect, Error}
    end.

do_unprepare_closing(Transport) ->
    case pgsql_transport:recv(Transport, ?recv_timeout) of
        {ok, #msg_close_complete{}, Transport1} ->
            do_sync(Transport1, ok);
        {ok, #msg_error_response{fields = Details}, Transport1} ->
            do_sync(Transport1, {error, Details});
        {error, _} = Error ->
            {disconnected, Error}
    end.

%% Execute protocol

do_execute(Transport, Name, Params) ->
    Messages = [
        #msg_bind{
            statement = Name,
            portal = "",
            parameters = Params,
            results = [binary]
        },
        #msg_execute{portal = "", limit = 0},
        #msg_sync{}
    ],
    case pgsql_transport:send(Transport, Messages) of
        ok -> do_execute_binding(Transport);
        {error, _} = Error -> {disconnect, Error}
    end.

do_execute_binding(Transport) ->
    case pgsql_transport:recv(Transport, ?recv_timeout) of
        {ok, #msg_bind_complete{}, Transport1} ->
            do_execute_receiving_rows(Transport1, []);
        {ok, #msg_error_response{fields = Details}, Transport1} ->
            do_sync(Transport1, {error, Details});
        {error, _} = Error ->
            {disconnected, Error}
    end.

do_execute_receiving_rows(Transport, Acc) ->
    case pgsql_transport:recv(Transport, ?recv_timeout) of
        {ok, #msg_no_data{}, Transport1} ->
            do_execute_receiving_rows(Transport1, []);
        {ok, #msg_data_row{values = Values}, Transport1} ->
            do_execute_receiving_rows(Transport1, [Values | Acc]);
        {ok, #msg_command_complete{tag = Tag}, Transport1} ->
            do_sync(Transport1, {ok, Tag, lists:reverse(Acc)});
        {ok, #msg_empty_query_response{}, Transport1} ->
            do_sync(Transport1, {ok, <<>>, []});
        {ok, #msg_error_response{fields = Details}, Transport1} ->
            do_sync(Transport1, {error, Details});
        {error, _} = Error ->
            {disconnected, Error}
    end.

%% Sync protocol

do_sync(Transport, Result) ->
    case pgsql_transport:recv(Transport, ?recv_timeout) of
        {ok, #msg_ready_for_query{}, Transport1} ->
            {done, Result, Transport1};
        {ok, _, Transport1} ->
            do_sync(Transport1, Result);
        {error, _} = Error ->
            {disconnected, Error}
    end.
