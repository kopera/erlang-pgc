-module(pgsql_connection).
-export([
    start_link/2
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
    handle_query/4,
    handle_unprepare/3,
    handle_info/2,
    disconnect/2
]).

-include("./pgsql_protocol.hrl").
-define(recv_timeout, 5000).
-record(state, {
    transport :: pgsql_transport:transport(),
    backend_key :: {pos_integer(), pos_integer()},
    parameters :: #{binary() => binary()},
    buffer = <<>> :: binary()
}).

-record(query, {
    name = <<>> :: binary(),
    parameters = [] :: [pgsql_protocol:oid()],
    fields = [] :: [#msg_row_description_field{}]
}).


start_link(TransportOpts, DatabaseOpts) ->
    gen_db_client_connection:start_link(?MODULE, {TransportOpts, DatabaseOpts}).

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
                parameters = Parameters
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
    case prepare(Transport, Name, Statement) of
        {done, {ok, Reply}, Transport1} ->
            {ok, Reply, State#state{transport = Transport1}};
        {done, {error, Reason}, Transport1} ->
            {error, Reason, State#state{transport = Transport1}};
        {disconnected, Reason} ->
            {disconnect, Reason}
    end.

handle_execute(_PreparedQuery, _Params, _Opts, State) ->
    {error, not_implemented, State}.

handle_unprepare(_PreparedQuery, _Opts, State) ->
    {error, not_implemented, State}.

handle_query(Query, Params, Opts, State) ->
    {error, not_implemented, State}.

handle_info(_Msg, State) ->
    {ok, State}.

disconnect(_Info, #state{transport = Transport} = State) ->
    _ = pgsql_transport:close(Transport),
    ok.


%% Prepare protocol

prepare(Transport, Name, Statement) ->
    Messages = [
        #msg_parse{name = Name, statement = Statement},
        #msg_describe{type = statement, name = Name},
        #msg_sync{}
    ],
    case pgsql_transport:send(Transport, Messages) of
        ok -> prepare_parsing(Transport, Name);
        {error, _} = Error -> {disconnect, Error}
    end.

prepare_parsing(Transport, Name) ->
    case pgsql_transport:recv(Transport, ?recv_timeout) of
        {ok, #msg_parse_complete{}, Transport1} ->
            prepare_describing_parameters(Transport1, Name);
        {ok, #msg_error_response{fields = Details}} ->
            sync(Transport, {error, Details});
        {error, _} = Error ->
            {disconnected, Error}
    end.

prepare_describing_parameters(Transport, Name) ->
    case pgsql_transport:recv(Transport, ?recv_timeout) of
        {ok, #msg_parameter_description{types = ParametersTypes}, Transport1} ->
            prepare_describing_row(Transport1, Name, ParametersTypes);
        {ok, #msg_error_response{fields = Details}} ->
            sync(Transport, {error, Details});
        {error, _} = Error ->
            {disconnected, Error}
    end.

prepare_describing_row(Transport, Name, ParametersTypes) ->
    case pgsql_transport:recv(Transport, ?recv_timeout) of
        {ok, #msg_row_description{fields = RowDescription}, Transport1} ->
            sync(Transport1, {ok, #query{name = Name, parameters = ParametersTypes, fields = RowDescription}});
        {ok, #msg_no_data{}} ->
            sync(Transport, {ok, #query{name = Name, parameters = ParametersTypes}});
        {error, _} = Error ->
            {disconnected, Error}
    end.

%% Execute protocol
execute(Transport, #query{name = Name, parameters = ParametersTypes, fields = FieldsTypes}, Parameters, Types) ->
    ok = pgsql_transport:send(Transport, [
        #msg_bind{
            statement = Name,
            portal = "",
            parameters = lists:zipwith(fun
                (null, _) ->
                    {binary, null};
                (Parameter, ParameterType) ->
                    {binary, pgsql_query:encode({Parameter, ParameterType}, Types, [])}
            end, Parameters, ParametersTypes),
            results = [binary]
        },
        #msg_execute{portal = "", limit = 0},
        #msg_sync{}
    ]),
    execute_binding(Connection, RowDescription, Types).

%% Sync protocol

sync(Transport, Result) ->
    case pgsql_transport:recv(Transport, ?recv_timeout) of
        {ok, #msg_ready_for_query{}, Transport1} ->
            {done, Result, Transport};
        {ok, _, Transport1} ->
            sync(Transport1, Result);
        {error, _} = Error ->
            {disconnected, Error}
    end.
