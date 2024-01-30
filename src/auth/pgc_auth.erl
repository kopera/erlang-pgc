%% @private
-module(pgc_auth).
-export([
    init/2,
    handle/3
]).
-export_type([
    state/0
]).

-include("../protocol/pgc_message.hrl").

-record(auth, {
    username :: unicode:unicode_binary(),
    password_fun :: fun(() -> unicode:unicode_binary())
}).
-record(auth_sasl, {
    handler :: module(),
    handler_state :: term()
}).
-type state() :: #auth{} | #auth_sasl{}.


init(Username, PasswordFun) ->
    #auth{
        username = Username,
        password_fun = PasswordFun
    }.

handle(cleartext, _, #auth{password_fun = PasswordFun} = State) ->
    Password = PasswordFun(),
    Response = #msg_password{password = Password},
    {ok, Response, State};
handle(md5, Salt, #auth{username = Username, password_fun = PasswordFun} = State) ->
    % concat('md5', md5(concat(md5(concat(password, username)), random-salt)))
    Password = [<<"md5">>, md5_hex([md5_hex([PasswordFun(), Username]), Salt])],
    Response = #msg_password{password = Password},
    {ok, Response, State};
handle(sasl, AuthData, #auth{username = Username, password_fun = PasswordFun}) ->
    Mechanisms = binary:split(AuthData, <<0>>, [global, trim]),
    case lists:search(fun is_mechanism_supported/1, Mechanisms) of
        {value, <<"SCRAM-SHA-256">> = Mechanism} ->
            {ok, InitialResponse, HandlerState} = pgc_auth_scram:init(sha256, Username, PasswordFun),
            State = #auth_sasl{
                handler = pgc_auth_scram,
                handler_state = HandlerState
            },
            Response = #msg_sasl_initial_response{
                mechanism = Mechanism,
                data = InitialResponse
            },
            {ok, Response, State};
        false ->
            {error, pgc_error:feature_not_supported({"unsupported SASL authentication mechanisms: ~w", [Mechanisms]})}
    end;
handle(sasl_continue, AuthData, #auth_sasl{handler = Handler, handler_state = HandlerState} = State) ->
    case Handler:continue(AuthData, HandlerState) of
        {ok, SASLResponse, HandlerState1} ->
            Response = #msg_sasl_response{
                data = SASLResponse
            },
            {ok, Response, State#auth_sasl{handler_state = HandlerState1}};
        {error, Reason} ->
            {error, pgc_error:authentication_failure({"sasl authentication failed with error: ~w", Reason})}
    end;
handle(sasl_final, AuthData, #auth_sasl{handler = Handler, handler_state = HandlerState}) ->
    case Handler:continue(AuthData, HandlerState) of
        ok ->
            ok;
        {error, Reason} ->
            {error, pgc_error:authentication_failure({"sasl authentication failed with error: ~w", Reason})}
    end;

handle(AuthType, _, #auth{}) ->
    {error, pgc_error:feature_not_supported({"unsupported authentication method: ~s", [AuthType]})}.


% ------------------------------------------------------------------------------
% Helpers
% ------------------------------------------------------------------------------

md5_hex(Data) ->
    binary:encode_hex(crypto:hash(md5, Data), lowercase).


%% @private
is_mechanism_supported(<<"SCRAM-SHA-256">>) ->
    true;
is_mechanism_supported(_) ->
    false.