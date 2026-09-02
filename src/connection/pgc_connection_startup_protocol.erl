-module(pgc_connection_startup_protocol).
-on_load(ensure_deps_loaded/0).

-moduledoc """
The PostgreSQL startup sub-protocol: send `StartupMessage`, respond to whatever
`AuthenticationXXX` challenge the server asks for (delegating the mechanics to
`pgc_auth`), and wait for `ReadyForQuery`.

`pgc_connection` `push_callback_module`s into this module right after the transport
connects, constructing this module's initial `t:#startup{}` state itself. This module
`pop_callback_module`s back to `pgc_connection`'s `#pgc_connection:ready{}` state on
success; any failure hands control back via `pgc_connection:stop_with_error/2`, per the
"every stop flows back through root" rule.
""".

-behaviour(gen_statem).
-export([
    init/1,
    callback_mode/0,
    handle_event/4,
    terminate/3,
    format_status/1
]).

-export_record([
    startup
]).


% ------------------------------------------------------------------------------
% State
% ------------------------------------------------------------------------------

-record #startup{
    auth_state :: pgc_auth:state()
}.

% ------------------------------------------------------------------------------
% gen_statem callbacks
% ------------------------------------------------------------------------------

-doc false.
init({Username, PasswordFun, Database, Parameters, Data}) ->
    StartupMessage = #pgc_protocol_message:startup{
        version = {3, 2},
        parameters = Parameters#{
            user => Username,
            database => Database
        }
    },
    {ok, #pgc_connection_startup_protocol:startup{
        auth_state = pgc_auth:init(Username, PasswordFun)
    }, Data, [
        {next_event, internal, #pgc_connection:send{messages = [StartupMessage]}}
    ]}.

-doc false.
callback_mode() ->
    [handle_event_function, state_enter].

-doc false.
terminate(Reason, State, Data) ->
    pgc_connection:terminate(Reason, State, Data).

-doc false.
format_status(Status) ->
    pgc_connection:format_status(Status).


% -------------------------------------------------------------------------------
% State: startup
% -------------------------------------------------------------------------------

handle_event(enter, _, #startup{}, _Data) ->
    keep_state_and_data;

handle_event(internal, #pgc_protocol_message:auth{type = ok}, #startup{}, _Data) ->
    keep_state_and_data;

handle_event(internal, #pgc_protocol_message:auth{} = Event, #startup{} = State, Data) ->
    #startup{
        auth_state = AuthState0
    } = State,
    #pgc_protocol_message:auth{
        type = AuthType,
        data = AuthData
    } = Event,
    case pgc_auth:handle(AuthType, AuthData, AuthState0) of
        ok ->
            keep_state_and_data;
        {ok, Response, AuthState1} ->
            {next_state, State#startup{auth_state = AuthState1}, Data, [
                {next_event, internal, #pgc_connection:send{messages = [Response]}}
            ]};
        {error, AuthError} ->
            pgc_connection:stop_with_error(AuthError, Data)
    end;

handle_event(internal, #pgc_protocol_message:negotiate_protocol_version{}, #startup{}, _Data) ->
    % We ask for 3.2, but might get downgraded to 3.0 -- message encoding handles both.
    keep_state_and_data;

handle_event(internal, #pgc_protocol_message:backend_key_data{id = Id, secret = Secret}, _State, Data) ->
    {keep_state, Data#pgc_connection:connection{backend_key = {Id, Secret}}};

handle_event(internal, #pgc_protocol_message:error_response{fields = Fields}, #startup{}, Data) ->
    pgc_connection:stop_with_error(pgc_protocol:from_error_response_fields(Fields), Data);

handle_event(internal, #pgc_protocol_message:ready_for_query{status = Status}, #startup{}, Data) ->
    #pgc_connection:connection{
        handler_module = Module,
        handler_state = HandlerState0,
        % backend_key = BackendKey,
        backend_parameters = BackendParameters
    } = Data,
    ConnectionInfo = #{
        % backend_key => BackendKey,
        parameters => BackendParameters
    },
    {ok, HandlerState1} = Module:handle_connected(ConnectionInfo, HandlerState0),
    {next_state, #pgc_connection:ready{
        status = Status
    }, Data#pgc_connection:connection{
        handler_state = HandlerState1
    }, [
        pop_callback_module
    ]};


% -------------------------------------------------------------------------------
% state: * -- anything not specific to the startup handshake.
% -------------------------------------------------------------------------------

handle_event(Type, Content, State, Data) ->
    pgc_connection:handle_common_event(Type, Content, State, Data).


% -------------------------------------------------------------------------------
% helpers
% -------------------------------------------------------------------------------

ensure_deps_loaded() ->
    {module, _} = code:ensure_loaded(pgc_connection),
    {module, _} = code:ensure_loaded(pgc_protocol_message),
    ok.

