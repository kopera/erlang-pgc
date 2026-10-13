-module(pgc_connection_statem_termination).
-moduledoc false.

-export([
    enter/3
]).
-export_type([
    reason/0
]).

-behaviour(gen_statem).
-export([
    init/1,
    callback_mode/0,
    handle_event/4,
    terminate/3,
    format_status/1
]).

-import_record(pgc_connection_statem, [data, send, callback]).


% -----------------------------------------------------------------------------
% Types
% -----------------------------------------------------------------------------

-type mode() ::
    immediate
    | graceful.

-type reason() ::
    normal
    | {auth_failure, pgc_protocol_message:error_response_fields() | pgc_connection_statem_auth_sasl:error()}
    | {error_response, pgc_protocol_message:error_response_fields()}
    | {unsupported_auth_type, atom() | byte() | {sasl, [binary()]}}
    | {transport_error, pgc_transport:error()}
    | ping_timeout.

% States ----------------------------------------------------------------------

-record #s_stopping{
    mode :: mode(),
    reason :: reason()
}.

% ------------------------------------------------------------------------------
% API
% ------------------------------------------------------------------------------

-spec enter(Mode, Reason, ConnectionData) -> gen_statem:event_handler_result(#s_stopping{}, ConnectionData) when
    Mode :: mode(),
    Reason :: reason(),
    ConnectionData :: #data{}.
enter(Mode, Reason, ConnectionData) ->
    {ok, NextState, NextData, Actions} = init({
        Mode,
        Reason,
        ConnectionData
    }),
    {next_state, NextState, NextData, [
        {change_callback_module, ?MODULE} | Actions
    ]}.

% ------------------------------------------------------------------------------
% gen_statem callbacks
% ------------------------------------------------------------------------------

-doc false.
-spec init({Mode, Reason, ConnectionData}) -> {ok, #s_stopping{}, ConnectionData, [gen_statem:action()]} when
    Mode :: mode(),
    Reason :: reason(),
    ConnectionData :: #data{}.
init({Mode, Reason, ConnectionData}) ->
    {ok, #s_stopping{mode = Mode, reason = Reason}, ConnectionData, []}.

-doc false.
callback_mode() ->
    [handle_event_function, state_enter].

-doc false.
terminate(_Reason, #s_stopping{reason = StopReason}, ConnectionData) ->
    #data{
        transport = Transport,

        handler_module = Handler,
        handler_state = HandlerState
    } = ConnectionData,
    case Transport of
        undefined -> ok;
        _ -> pgc_transport:close(Transport)
    end,
    case erlang:function_exported(Handler, terminate, 2) of
        true -> Handler:terminate(StopReason, HandlerState);
        false -> ok
    end.

-doc false.
format_status(Status) ->
    pgc_connection_statem_common:format_status(Status).


% -------------------------------------------------------------------------------
% State: stopping
% -------------------------------------------------------------------------------

-doc false.
handle_event(enter, _OldState, #s_stopping{}, #data{transport = undefined}) ->
    {stop, normal};

handle_event(enter, _OldState, #s_stopping{mode = immediate}, #data{transport = Transport}) when Transport =/= undefined ->
    {stop, normal};

handle_event(enter, _OldState, #s_stopping{}, #data{transport = Transport} = ConnectionData) when Transport =/= undefined ->
    case pgc_transport:send(Transport, pgc_protocol_messages:encode([#pgc_protocol_message:terminate{}])) of
        ok ->
            {keep_state_and_data, [
                {state_timeout, ConnectionData#data.ping_interval, stop}
            ]};
        _ ->
            {stop, normal}
    end;

handle_event(state_timeout, stop, #s_stopping{}, _ConnectionData) ->
    {stop, normal};

handle_event(info, Info, _State, #data{transport = Transport}) when Transport =/= undefined ->
    case pgc_transport:handle_message(Transport, Info) of
        {error, _TransportError} ->
            {stop, normal};
        _ ->
            keep_state_and_data
    end;

handle_event(_, _, _State, _ConnectionData) ->
    keep_state_and_data.
