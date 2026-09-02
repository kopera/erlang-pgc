-module(pgc_auth).
-on_load(ensure_deps_loaded/0).

-export([
    init/2,
    handle/3
]).
-export_record([
    error
]).
-export_type([
    state/0,
    error/0,
    error_reason/0
]).


% ------------------------------------------------------------------------------
% Errors
% ------------------------------------------------------------------------------

-record #error{
    reason :: error_reason()
}.
-type error() :: #error{}.
-type error_reason() ::
    {unsupported_method, atom() | byte()}
    | {unsupported_mechanisms, [unicode:unicode_binary()]}
    | {authentication_failure, term()}.

% ------------------------------------------------------------------------------
% Types
% ------------------------------------------------------------------------------

-record #state{
    username :: unicode:chardata(),
    password_fun :: fun(() -> unicode:chardata())
}.

-record #state_sasl{
    handler :: module(),
    handler_state :: term()
}.

-opaque state() :: #state{} | #state_sasl{}.


% ------------------------------------------------------------------------------
% API
% ------------------------------------------------------------------------------

-doc false.
-spec init(Username, PasswordFun) -> State when
    Username :: unicode:chardata(),
    PasswordFun :: fun(() -> unicode:chardata()),
    State :: state().
init(Username, PasswordFun) ->
    #state{
        username = Username,
        password_fun = PasswordFun
    }.

-doc false.
-spec handle(Method, Data, State) -> ok | {ok, Response, State} | {error, #error{}} when
    Method :: atom() | byte(),
    Data :: binary(),
    Response :: pgc_protocol_message:message_f(),
    State :: state().
handle(cleartext, _, #state{password_fun = PasswordFun} = State) ->
    Password = PasswordFun(),
    Response = #pgc_protocol_message:password{password = Password},
    {ok, Response, State};
handle(md5, Salt, #state{username = Username, password_fun = PasswordFun} = State) ->
    % concat('md5', md5(concat(md5(concat(password, username)), random-salt)))
    Password = [<<"md5">>, md5_hex([md5_hex([PasswordFun(), Username]), Salt])],
    Response = #pgc_protocol_message:password{password = Password},
    {ok, Response, State};
handle(sasl, AuthData, #state{username = Username, password_fun = PasswordFun}) ->
    Mechanisms = binary:split(AuthData, <<0>>, [global, trim]),
    case lists:search(fun is_mechanism_supported/1, Mechanisms) of
        {value, <<"SCRAM-SHA-256">> = Mechanism} ->
            {ok, InitialResponse, HandlerState} = pgc_auth_scram:init([sha256, Username, PasswordFun]),
            State = #state_sasl{
                handler = pgc_auth_scram,
                handler_state = HandlerState
            },
            Response = #pgc_protocol_message:sasl_initial_response{
                mechanism = Mechanism,
                data = InitialResponse
            },
            {ok, Response, State};
        false ->
            {error, #error{reason = {unsupported_mechanisms, Mechanisms}}}
    end;
handle(sasl_continue, AuthData, #state_sasl{handler = Handler, handler_state = HandlerState} = State) ->
    case Handler:continue(AuthData, HandlerState) of
        {ok, SASLResponse, HandlerState1} ->
            Response = #pgc_protocol_message:sasl_response{
                data = SASLResponse
            },
            {ok, Response, State#state_sasl{handler_state = HandlerState1}};
        % elp:ignore W0027
        {error, Reason} ->
            {error, #error{reason = {authentication_failure, [Reason]}}}
    end;
handle(sasl_final, AuthData, #state_sasl{handler = Handler, handler_state = HandlerState}) ->
    case Handler:continue(AuthData, HandlerState) of
        ok ->
            ok;
        % elp:ignore W0027
        {error, Reason} ->
            {error, #error{reason = {authentication_failure, [Reason]}}}
    end;

handle(AuthType, _, #state{}) ->
    {error, #error{reason = {unsupported_method, AuthType}}}.


% ------------------------------------------------------------------------------
% Helpers
% ------------------------------------------------------------------------------

ensure_deps_loaded() ->
    {module, pgc_protocol_message} = code:ensure_loaded(pgc_protocol_message),
    ok.

md5_hex(Data) ->
    binary:encode_hex(crypto:hash(md5, Data), lowercase).


is_mechanism_supported(<<"SCRAM-SHA-256">>) ->
    true;
is_mechanism_supported(_) ->
    false.
