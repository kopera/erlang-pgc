-module(pgc_connection_statem_startup).
-moduledoc false.

-export([
    enter/5,
    continue/3
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
-import_record(pgc_protocol_message, [startup, auth, negotiate_protocol_version, backend_key_data, error_response, ready_for_query]).


% -----------------------------------------------------------------------------
% Types
% -----------------------------------------------------------------------------

% States ----------------------------------------------------------------------

-record #s_startup {
    user :: unicode:chardata(),
    password :: fun(() -> unicode:chardata())
}.

% ------------------------------------------------------------------------------
% API
% ------------------------------------------------------------------------------

-spec enter(User, Password, Database, Parameters, ConnectionData) -> gen_statem:event_handler_result(#s_startup{}, ConnectionData) when
    User :: unicode:chardata(),
    Password :: fun(() -> unicode:chardata()),
    Database :: unicode:chardata(),
    Parameters :: #{
        atom() => unicode:chardata()
    },
    ConnectionData :: #data{}.
enter(User, Password, Database, Parameters, ConnectionData) ->
    {ok, NextState, NextData, Actions} = init({
        User,
        Password,
        Database,
        Parameters,
        ConnectionData
    }),
    {next_state, NextState, NextData, [
        {change_callback_module, ?MODULE} | Actions
    ]}.


-spec continue(User, Password, ConnectionData) -> gen_statem:event_handler_result(#s_startup{}, ConnectionData) when
    User :: unicode:chardata(),
    Password :: fun(() -> unicode:chardata()),
    ConnectionData :: #data{}.
continue(User, Password, ConnectionData) ->
    {next_state, #s_startup{
        user = User,
        password = Password
    }, ConnectionData, [
        {change_callback_module, ?MODULE}
    ]}.


% -----------------------------------------------------------------------------
% gen_statem callbacks
% -----------------------------------------------------------------------------

-doc false.
-spec init({User, Password, Database, Parameters, ConnectionData}) -> {ok, #s_startup{}, ConnectionData, [gen_statem:action()]} when
    User :: unicode:chardata(),
    Password :: fun(() -> unicode:chardata()),
    Database :: unicode:chardata(),
    Parameters :: #{
        atom() => unicode:chardata()
    },
    ConnectionData :: #data{}.
init({User, Password, Database, Parameters, ConnectionData}) ->
    {ok, #s_startup{
        user = User,
        password = Password
    }, ConnectionData, [
        {next_event, internal, #send{
            messages = [
                #startup{
                    version = {3, 2},
                    parameters = Parameters#{
                        user => User,
                        database => Database
                    }
                }
            ]
        }}
    ]}.

-doc false.
callback_mode() ->
    [handle_event_function].

-doc false.
terminate(Reason, State, ConnectionData) ->
    pgc_connection_statem_common:terminate(Reason, State, ConnectionData).

-doc false.
format_status(Status) ->
    pgc_connection_statem_common:format_status(Status).


% -------------------------------------------------------------------------------
% State: startup
% -------------------------------------------------------------------------------

handle_event(internal, #auth{type = ok}, #s_startup{}, _ConnectionData) ->
    keep_state_and_data;

handle_event(internal, #auth{type = md5, data = Salt}, #s_startup{} = State, ConnectionData) ->
    #s_startup{
        user = User,
        password = Password
    } = State,
    pgc_connection_statem_auth_md5:enter(User, Password, Salt, ConnectionData);

handle_event(internal, #auth{type = sasl, data = InitialData}, #s_startup{} = State, ConnectionData) ->
    #s_startup{
        user = User,
        password = Password
    } = State,
    pgc_connection_statem_auth_sasl:enter(User, Password, InitialData, ConnectionData);

handle_event(internal, #auth{type = Type}, #s_startup{}, ConnectionData) ->
    pgc_connection_statem_termination:enter(immediate, {unsupported_auth_type, Type}, ConnectionData);

handle_event(internal, #negotiate_protocol_version{}, #s_startup{}, _ConnectionData) ->
    % We ask for 3.2, but might get downgraded to 3.0 -- message encoding handles both.
    keep_state_and_data;

handle_event(internal, #backend_key_data{id = Id, secret = Secret}, _State, ConnectionData) ->
    {keep_state, ConnectionData#data{
        backend_key = {Id, Secret}
    }};

handle_event(internal, #error_response{fields = Fields}, #s_startup{}, ConnectionData) ->
    pgc_connection_statem_termination:enter(immediate, {error_response, Fields}, ConnectionData);

handle_event(internal, #ready_for_query{status = Status}, #s_startup{}, ConnectionData) ->
    pgc_connection_statem:ready(Status, ConnectionData);

handle_event({call, _}, _, #s_startup{}, _ConnectionData) ->
    {keep_state_and_data, [postpone]};

handle_event(cast, _, #s_startup{}, _ConnectionData) ->
    {keep_state_and_data, [postpone]};

% -------------------------------------------------------------------------------
% state: *
% -------------------------------------------------------------------------------

handle_event(Type, Content, State, ConnectionData) ->
    pgc_connection_statem_common:handle_event(Type, Content, State, ConnectionData).
