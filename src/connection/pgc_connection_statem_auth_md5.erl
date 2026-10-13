-module(pgc_connection_statem_auth_md5).
-moduledoc false.

-export([
    enter/4
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
-import_record(pgc_protocol_message, [password, auth, error_response]).


% -----------------------------------------------------------------------------
% Types
% -----------------------------------------------------------------------------

% States ----------------------------------------------------------------------

-record #s_auth_md5 {
    user :: unicode:chardata(),
    password :: fun(() -> unicode:chardata())
}.

% ------------------------------------------------------------------------------
% API
% ------------------------------------------------------------------------------

-spec enter(User, Password, Salt, ConnectionData) -> gen_statem:event_handler_result(#s_auth_md5{}, ConnectionData) when
    User :: unicode:chardata(),
    Password :: fun(() -> unicode:chardata()),
    Salt :: binary(),
    ConnectionData :: #data{}.
enter(User, Password, Salt, ConnectionData) ->
    {ok, NextState, NextData, Actions} = init({
        User,
        Password,
        Salt,
        ConnectionData
    }),
    {next_state, NextState, NextData, [
        {change_callback_module, ?MODULE} | Actions
    ]}.


% -----------------------------------------------------------------------------
% gen_statem callbacks
% -----------------------------------------------------------------------------

-doc false.
-spec init({User, Password, Salt, ConnectionData}) -> {ok, #s_auth_md5{}, ConnectionData, [gen_statem:action()]} when
    User :: unicode:chardata(),
    Password :: fun(() -> unicode:chardata()),
    Salt :: binary(),
    ConnectionData :: #data{}.
init({User, Password, Salt, ConnectionData}) ->
    {ok, #s_auth_md5{
        user = User,
        password = Password
    }, ConnectionData, [
        {next_event, internal, #send{
            messages = [
                #password{
                    % concat('md5', md5(concat(md5(concat(password, username)), random-salt)))
                    password = [<<"md5">>, md5_hex([md5_hex([Password(), User]), Salt])]
                }
            ]
        }}
    ]}.

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

handle_event(internal, #auth{type = ok}, #s_auth_md5{} = State, ConnectionData) ->
    #s_auth_md5{
        user = User,
        password = Password
    } = State,
    pgc_connection_statem_startup:continue(User, Password, ConnectionData);

handle_event(internal, #error_response{fields = Fields}, #s_auth_md5{}, ConnectionData) ->
    pgc_connection_statem_termination:enter(immediate, {auth_failure, Fields}, ConnectionData);

handle_event({call, _}, _, #s_auth_md5{}, _Data) ->
    {keep_state_and_data, [postpone]};

handle_event(cast, _, #s_auth_md5{}, _Data) ->
    {keep_state_and_data, [postpone]};

% -------------------------------------------------------------------------------
% state: * -- anything not specific to the startup handshake.
% -------------------------------------------------------------------------------

handle_event(Type, Content, _State, Data) ->
    pgc_connection_statem_common:handle_event(Type, Content, connect, Data).


% ------------------------------------------------------------------------------
% Helpers
% ------------------------------------------------------------------------------

md5_hex(Data) ->
    binary:encode_hex(crypto:hash(md5, Data), lowercase).
