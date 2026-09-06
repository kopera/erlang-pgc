-module(pgc_connection_statem_common).
-moduledoc false.

-export([
    handle_event/4,
    terminate/3,
    format_status/1
]).

-import_record(pgc_connection_statem, [data, send, callback, query, prepare, unprepare, execute, cancel]).
-import_record(pgc_protocol_message, [parameter_status, notice_response, notification_response]).

% -----------------------------------------------------------------------------
% API
% -----------------------------------------------------------------------------

-doc false.
handle_event(internal, #query{}, _State, #data{}) ->
    % Always postpone until handled by the ready state
    {keep_state_and_data, [postpone]};

handle_event(internal, #prepare{}, _State, #data{}) ->
    % Always postpone until handled by the ready state
    {keep_state_and_data, [postpone]};

handle_event(internal, #unprepare{}, _State, #data{}) ->
    % Always postpone until handled by the ready state
    {keep_state_and_data, [postpone]};

handle_event(internal, #execute{}, _State, #data{}) ->
    % Always postpone until handled by the ready state
    {keep_state_and_data, [postpone]};

handle_event(internal, #cancel{ref = Ref}, _State, #data{current_ref = CurrentRef}) when Ref =/= CurrentRef ->
    % Stale, or not-yet-dispatched -- whatever this targets isn't what's on the wire. No-op, same
    % as Postgres's own CancelRequest semantics when nothing matching is currently executing.
    keep_state_and_data;

handle_event(internal, #cancel{}, _State, #data{transport = undefined}) ->
    keep_state_and_data;

handle_event(internal, #cancel{}, _State, #data{backend_key = undefined}) ->
    logger:warning("pgc_connection: cancel request failed: not supported"),
    keep_state_and_data;

handle_event(internal, #cancel{}, _State, #data{transport = Transport, backend_key = {Id, Secret}}) ->
    _ = spawn(fun () -> send_cancel_request(Transport, Id, Secret) end),
    keep_state_and_data;

handle_event(internal, #send{}, _State, #data{transport = undefined}) ->
    keep_state_and_data;

handle_event(internal, #send{messages = Messages}, _State, #data{transport = Transport} = ConnectionData) when Transport =/= undefined ->
    case pgc_transport:send(Transport, pgc_protocol_messages:encode(Messages)) of
        ok ->
            keep_state_and_data;
        {error, Error} ->
            pgc_connection_statem_termination:enter(immediate, {transport_error, Error}, ConnectionData)
    end;

handle_event(internal, #callback{name = CallbackName, args = CallbackArgs0}, State, ConnectionData)->
    #data{
        handler_module = HandlerModule,
        handler_state = HandlerState0
    } = ConnectionData,
    CallbackArgs = [connection_info(State, ConnectionData) | CallbackArgs0] ++ [HandlerState0],
    Arity = length(CallbackArgs),
    {CallbackActions, HandlerState1} = case erlang:function_exported(HandlerModule, CallbackName, Arity) of
        true ->
            erlang:apply(HandlerModule, CallbackName, CallbackArgs);
        false ->
            {[], HandlerState0}
    end,
    {keep_state, ConnectionData#data{handler_state = HandlerState1}, [case CallbackAction of
        {reply, _, _} = Reply ->
            Reply;
        {query, Ref, Text} ->
            {next_event, internal, #query{ref = Ref, text = Text}};
        {prepare, Ref, Name, Text} ->
            {next_event, internal, #prepare{ref = Ref, name = Name, text = Text}};
        {unprepare, Ref, Name} ->
            {next_event, internal, #unprepare{ref = Ref, name = Name}};
        {execute, Ref, Name, Parameters, Options} ->
            {next_event, internal, #execute{ref = Ref, name = Name, parameters = Parameters, options = Options}};
        {cancel, Ref} ->
            {next_event, internal, #cancel{ref = Ref}}
    end || CallbackAction <- CallbackActions]};

handle_event(internal, #parameter_status{name = Name, value = Value}, _State, ConnectionData) ->
    #data{
        backend_parameters = BackendParameters
    } = ConnectionData,
    {keep_state, ConnectionData#data{
        backend_parameters = BackendParameters#{Name => Value}
    }};

handle_event(internal, #notice_response{fields = Fields}, _State, _ConnectionData) ->
    {keep_state_and_data, [
        {next_event, internal, #callback{name = handle_notice, args = [Fields]}}
    ]};

handle_event(internal, #notification_response{id = SenderId, channel = Channel, payload = Payload}, _State, _ConnectionData) ->
    {keep_state_and_data, [
        {next_event, internal, #callback{name = handle_notification, args = [SenderId, Channel, Payload]}}
    ]};

handle_event({call, From}, Request, _State, _Data) ->
    {keep_state_and_data, [
        {next_event, internal, #callback{name = handle_call, args = [Request, From]}}
    ]};

handle_event(cast, Request, _State, _Data) ->
    {keep_state_and_data, [
        {next_event, internal, #callback{name = handle_cast, args = [Request]}}
    ]};

% handle_event(info, {{'DOWN', owner}, OwnerMonitor, process, _Pid, _Reason}, _Phase, #data{owner_monitor = OwnerMonitor} = ConnectionData) ->
%     pgc_connection_statem_termination:enter(graceful, normal, ConnectionData);

handle_event(info, Info, _State, #data{transport = undefined} = _ConnectionData)->
    {keep_state_and_data, [
        {next_event, internal, #callback{name = handle_info, args = [Info]}}
    ]};

handle_event(info, Info, _State, #data{transport = Transport} = ConnectionData) when Transport =/= undefined ->
    #data{transport_buffer = Buffer} = ConnectionData,
    case pgc_transport:handle_message(Transport, Info) of
        {data, TransportData} ->
            {Messages, Rest} = pgc_protocol_messages:decode(<<Buffer/binary, TransportData/binary>>),
            ok = pgc_transport:set_active(Transport, once),
            {keep_state, ConnectionData#data{transport_buffer = Rest}, [
                {next_event, internal, Message} || Message <- Messages
            ]};
        {error, TransportError} ->
            pgc_connection_statem_termination:enter(immediate, {transport_error, TransportError}, ConnectionData#data{
                transport = undefined
            });
        unknown ->
            {keep_state_and_data, [
                {next_event, internal, #callback{name = handle_info, args = [Info]}}
            ]}
    end.


-doc false.
terminate(Reason, _State, #data{} = ConnectionData) ->
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
        true -> Handler:terminate(Reason, HandlerState);
        false -> ok
    end.

-doc false.
format_status(Status) ->
    Status.
    % maps:map(fun
    %     (data, #data{} = ConnectionData) ->
    %         ConnectionData#data{
    %             % This field can be very large
    %             %types = pgc_connection_types:new()
    %         };
    %     (_Key, Value) ->
    %         Value
    % end, Status).

% -----------------------------------------------------------------------------
% Helpers
% -----------------------------------------------------------------------------

-spec connection_info(term(), #data{}) -> pgc_connection:connection_info().
connection_info(_State, ConnectionData) ->
    #data{
        backend_parameters = BackendParameters
    } = ConnectionData,
    #{
        parameters => BackendParameters
    }.

-doc """
Cancels whatever the backend is currently executing, by opening a second, short-lived
connection to the same peer and sending a `CancelRequest` on it, per the PostgreSQL wire
protocol (a query's own socket can't be used to cancel itself). Runs in a spawned process
so a slow/unreachable peer can't stall the connection's main socket while this waits.
""".
send_cancel_request(Transport, Id, Secret) ->
    case pgc_transport:dup(Transport, 5000) of
        {ok, CancelTransport} ->
            _ = pgc_transport:send(CancelTransport, pgc_protocol_messages:encode([
                #pgc_protocol_message:cancel_request{id = Id, secret = Secret}
            ])),
            _ = pgc_transport:recv(CancelTransport, 5000),
            pgc_transport:close(CancelTransport);
        {error, Reason} ->
            logger:warning("pgc_connection: cancel request failed: ~p", [Reason])
    end.
