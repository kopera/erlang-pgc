-module(pgc_connection_statem_common).
-moduledoc false.

-export([
    handle_event/4,
    terminate/3,
    format_status/1
]).

-import_record(pgc_connection_statem, [data, send, callback, query, prepare, unprepare, execute]).
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
        {query, Text} ->
            {next_event, internal, #query{text = Text}};
        {prepare, Name, Text} ->
            {next_event, internal, #prepare{name = Name, text = Text}};
        {unprepare, Name} ->
            {next_event, internal, #unprepare{name = Name}};
        {execute, Name, Parameters, Options} ->
            {next_event, internal, #execute{name = Name, parameters = Parameters, options = Options}}
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

% handle_callback_actions([], State, ConnectionData) ->
%     {next_state, State, ConnectionData};
% handle_callback_actions([{reply, From, Reply} | Actions], State, ConnectionData) ->
%     gen_statem:reply(From, Reply),

%     case ConnectionData of
%         #data{transport = undefined} ->
%             handle_callback_actions(Actions, State, ConnectionData);
%         #data{backend_key = undefined} ->
%             logger:warning("pgc_connection: cancel request failed: not supported"),
%             handle_callback_actions(Actions, State, ConnectionData);
%         #data{transport = Transport, backend_key = {Id, Secret}} ->
%             maybe
%                 {ok, CancelTransport} ?= pgc_transport:dup(Transport, 5000),
%                 ok ?= pgc_transport:send(CancelTransport, pgc_protocol_messages:encode([
%                     #pgc_protocol_message:cancel_request{id = Id, secret = Secret}
%                 ])),
%                 _ = pgc_transport:recv(CancelTransport, 5000),
%                 ok = pgc_transport:close(CancelTransport)
%             else
%                 _ ->
%                     logger:warning("pgc_connection: cancel request failed: ~p", [Reason])
%             end,
%             handle_callback_actions(Actions, State, ConnectionData)
%     end.


    % case is_ready(Phase) andalso lists:keytake(query, 1, Actions) of
    %     {value, {query, Sql}, RestActions} ->
    %         {FilteredActions, Data1} = apply_side_effects(RestActions, Data0),
    %         {next_state, NextState, NextData, QueryActions} = start_simple_query_protocol(Sql, Data1),
    %         {next_state, NextState, NextData, QueryActions ++ gen_statem_actions(FilteredActions)};
    %     _ ->
    %         {FilteredActions, Data1} = apply_side_effects(Actions, Data0),
    %         {keep_state, Data1, gen_statem_actions(FilteredActions)}
    % end.
