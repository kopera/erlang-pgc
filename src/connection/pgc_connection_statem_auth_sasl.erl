-module(pgc_connection_statem_auth_sasl).
-moduledoc false.

-export([
    enter/4
]).
-export_type([
    error/0
]).

-behaviour(gen_statem).
-export([
    init/1,
    callback_mode/0,
    handle_event/4,
    terminate/3,
    format_status/1
]).

-import_record(pgc_connection_statem, [data, send]).
-import_record(pgc_protocol_message, [auth, sasl_initial_response, sasl_response, error_response]).


% -----------------------------------------------------------------------------
% Types
% -----------------------------------------------------------------------------

-type error() :: {scram, pgc_auth_scram:error()}.

% States ----------------------------------------------------------------------

-record #s_auth_sasl {
    user :: unicode:chardata(),
    password :: fun(() -> unicode:chardata()),
    mechanism :: {scram, pgc_auth_scram, pgc_auth_scram:state()}
}.

% ------------------------------------------------------------------------------
% API
% ------------------------------------------------------------------------------

-spec enter(User, Password, InitialData, ConnectionData) -> gen_statem:event_handler_result(#s_auth_sasl{}, ConnectionData) when
    User :: unicode:chardata(),
    Password :: fun(() -> unicode:chardata()),
    InitialData :: binary(),
    ConnectionData :: #data{}.
enter(User, Password, InitialData, ConnectionData) ->
    {ok, NextState, NextData, Actions} = init({
        User,
        Password,
        InitialData,
        ConnectionData
    }),
    {next_state, NextState, NextData, [
        {change_callback_module, ?MODULE} | Actions
    ]}.


% -----------------------------------------------------------------------------
% gen_statem callbacks
% -----------------------------------------------------------------------------

-doc false.
init({User, Password, InitialData, ConnectionData}) ->
    Mechanisms = binary:split(InitialData, <<0>>, [global, trim]),
    case lists:search(fun is_mechanism_supported/1, Mechanisms) of
        {value, ~"SCRAM-SHA-256" = Mechanism} ->
            {ok, ClientFirstMessage, MechanismState} = pgc_auth_scram:init(sha256, User, Password),
            {ok, #s_auth_sasl{
                user = User,
                password = Password,
                mechanism = {scram, pgc_auth_scram, MechanismState}
            }, ConnectionData, [
                {next_event, internal, #send{
                    messages = [
                        #sasl_initial_response{
                            mechanism = Mechanism,
                            data = ClientFirstMessage
                        }
                    ]
                }}
            ]};
        false ->
            pgc_connection_statem_termination:enter(immediate, {unsupported_auth_type, {sasl, Mechanisms}}, ConnectionData)
    end.

-doc false.
callback_mode() ->
    [handle_event_function].

-doc false.
terminate(Reason, State, Data) ->
    pgc_connection_statem_common:terminate(Reason, State, Data).

-doc false.
format_status(Status) ->
    pgc_connection_statem_common:format_status(Status).


% -------------------------------------------------------------------------------
% State: startup
% -------------------------------------------------------------------------------

handle_event(internal, #auth{type = ok}, #s_auth_sasl{} = State, ConnectionData) ->
    #s_auth_sasl{
        user = User,
        password = Password
    } = State,
    pgc_connection_statem_startup:continue(User, Password, ConnectionData);

handle_event(internal, #auth{type = sasl_continue, data = ServerMessage}, #s_auth_sasl{} = State, ConnectionData) ->
    #s_auth_sasl{
        mechanism = {MechanismName, MechanismHandler, MechanismHandlerState0}
    } = State,
    case MechanismHandler:handle_continue(ServerMessage, MechanismHandlerState0) of
        {ok, ClientResponse, MechanismHandlerState1} ->
            {next_state, State#s_auth_sasl{
                mechanism = {MechanismName, MechanismHandler, MechanismHandlerState1}
            }, ConnectionData, [
                {next_event, internal, #send{
                    messages = [
                        #sasl_response{
                            data = ClientResponse
                        }
                    ]
                }}
            ]};
        {error, Error} ->
            pgc_connection_statem_termination:enter(immediate, {auth_failure, {MechanismName, Error}}, ConnectionData)
    end;

handle_event(internal, #auth{type = sasl_final, data = ServerFinalMessage}, #s_auth_sasl{} = State, ConnectionData) ->
    #s_auth_sasl{
        mechanism = {MechanismName, MechanismHandler, MechanismHandlerState0}
    } = State,
    case MechanismHandler:handle_final(ServerFinalMessage, MechanismHandlerState0) of
        ok ->
            keep_state_and_data;
        {error, Error} ->
            pgc_connection_statem_termination:enter(immediate, {auth_failure, {MechanismName, Error}}, ConnectionData)
    end;

handle_event(internal, #error_response{fields = Fields}, #s_auth_sasl{}, ConnectionData) ->
    pgc_connection_statem_termination:enter(immediate, {auth_failure, Fields}, ConnectionData);

handle_event({call, _}, _, #s_auth_sasl{}, _Data) ->
    {keep_state_and_data, [postpone]};

handle_event(cast, _, #s_auth_sasl{}, _Data) ->
    {keep_state_and_data, [postpone]};

% -----------------------------------------------------------------------------
% state: *
% -----------------------------------------------------------------------------

handle_event(Type, Content, State, ConnectionData) ->
    pgc_connection_statem_common:handle_event(Type, Content, State, ConnectionData).


% -----------------------------------------------------------------------------
% Helpers
% -----------------------------------------------------------------------------

is_mechanism_supported(~"SCRAM-SHA-256") ->
    true;
is_mechanism_supported(_) ->
    false.
