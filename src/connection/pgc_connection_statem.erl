-module(pgc_connection_statem).
-moduledoc false.

-export([
    ready/2
]).


-behaviour(gen_statem).
-export([
    init/1,
    callback_mode/0,
    handle_event/4,
    terminate/3,
    format_status/1
]).
-export_record([
    args,
    data
]).
-export_record([
    send,
    callback,
    query,
    prepare,
    unprepare,
    execute,
    cancel
]).

% -----------------------------------------------------------------------------
% Types
% -----------------------------------------------------------------------------

-record #args{
    % owner :: pid(),

    address :: pgc_transport:address(),

    connect_options :: pgc_transport:connect_options(),
    connect_timeout :: timeout(),
    ping_interval :: timeout(),

    user :: unicode:chardata(),
    password :: fun(() -> unicode:chardata()),
    database :: unicode:chardata(),
    parameters :: #{
        atom() => unicode:chardata()
    },

    handler_module :: module(),
    handler_args :: term()
}.


% Data ------------------------------------------------------------------------

-record #data{
    % owner_monitor :: reference(),

    transport :: pgc_transport:t() | undefined,
    transport_buffer :: binary(),

    ping_interval :: timeout(),
    ping_timeout :: timeout(),

    backend_key :: {non_neg_integer(), binary()} | undefined,
    backend_parameters :: #{binary() => binary()},

    handler_module :: module(),
    handler_state :: term()
}.


% States ----------------------------------------------------------------------

-record #s_disconnected{
}.

-record #s_ready{
    status :: idle | transaction | error
}.

-record #s_pinging{
    status :: idle | transaction | error
}.

% Actions ----------------------------------------------------------------------

-record #connect{
    address :: pgc_transport:address(),
    connect_options :: pgc_transport:connect_options(),
    connect_timeout :: timeout(),

    user :: unicode:chardata(),
    password :: fun(() -> unicode:chardata()),
    database :: unicode:chardata(),
    parameters :: #{
        atom() => unicode:chardata()
    }
}.

-record #send{
    messages :: [pgc_protocol_message:message_f() | pgc_protocol_message:message_fb()]
}.

-record #callback{
    name :: handle_ready | handle_row_data | handle_query_result | handle_prepare_result | handle_unprepare_result | handle_execute_result | handle_notice | handle_notification | handle_call | handle_cast | handle_info,
    args :: [term()]
}.

-record #query{
    text :: unicode:chardata()
}.

-record #prepare{
    name :: unicode:chardata(),
    text :: unicode:chardata()
}.

-record #unprepare{
    name :: unicode:chardata()
}.

-record #execute{
    name :: unicode:chardata(),
    parameters :: pgc_connection_statem_extended_query:execute_parameters(),
    options :: pgc_connection_statem_extended_query:execute_options()
}.

-record #cancel{
}.

% ------------------------------------------------------------------------------
% ready
% ------------------------------------------------------------------------------

-spec ready(Status, ConnectionData) -> gen_statem:event_handler_result(#s_ready{}, ConnectionData) when
    Status :: idle | transaction | error,
    ConnectionData :: #data{}.
ready(Status, ConnectionData) ->
    {next_state, #s_ready{status = Status}, ConnectionData, [
        {change_callback_module, ?MODULE},
        {next_event, internal, #callback{name = handle_ready, args = []}}
    ]}.


% ------------------------------------------------------------------------------
% gen_statem callbacks
% ------------------------------------------------------------------------------

-doc false.
-spec init(Args) -> gen_statem:init_result() when
    Args :: #args{}.
init(#args{
    % owner = Owner,

    address = Address,
    connect_options = ConnectOptions,
    connect_timeout = ConnectTimeout,
    ping_interval = PingInterval,

    user = User,
    password = Password,
    database = Database,
    parameters = Parameters,

    handler_module = HandlerModule,
    handler_args = HandlerArgs
}) ->
    {ok, HandlerState} = HandlerModule:init(HandlerArgs),
    % OwnerMonitor = erlang:monitor(process, Owner, [
    %     {tag, {'DOWN', owner}}
    % ]),
    {ok, #s_disconnected{}, #data{
        % owner_monitor = OwnerMonitor,

        transport = undefined,
        transport_buffer = <<>>,

        ping_interval = PingInterval,
        ping_timeout = if
            is_integer(PingInterval), PingInterval > 0 ->
                2 * PingInterval;
            PingInterval =:= infinity ->
                infinity
        end,

        backend_key = undefined,
        backend_parameters = #{},

        handler_module = HandlerModule,
        handler_state = HandlerState
    }, [
        {next_event, internal, #connect{
            address = Address,
            connect_options = ConnectOptions,
            connect_timeout = ConnectTimeout,

            user = User,
            password = Password,
            database = Database,
            parameters = Parameters
        }}
    ]}.

-doc false.
callback_mode() ->
    [handle_event_function, state_enter].

-doc false.
terminate(Reason, State, ConnectionData) ->
    pgc_connection_statem_common:terminate(Reason, State, ConnectionData).

-doc false.
format_status(Status) ->
    pgc_connection_statem_common:format_status(Status).


% -------------------------------------------------------------------------------
% State: disconnected
% -------------------------------------------------------------------------------

handle_event(enter, _OldState, #s_disconnected{}, _ConnectionData) ->
    keep_state_and_data;

handle_event(internal, #connect{} = Connect, #s_disconnected{}, ConnectionData) ->
    #connect{
        address = Address,
        connect_options = ConnectOptions,
        connect_timeout = ConnectTimeout,

        user = User,
        password = Password,
        database = Database,
        parameters = Parameters
    } = Connect,
    case pgc_transport:connect(Address, ConnectOptions, ConnectTimeout) of
        {ok, Transport} ->
            ok = pgc_transport:set_active(Transport, once),
            pgc_connection_statem_startup:enter(User, Password, Database, Parameters, ConnectionData#data{
                transport = Transport
            });
        {error, ConnectError} ->
            pgc_connection_statem_termination:enter(immediate, {transport_error, ConnectError}, ConnectionData)
    end;

% -------------------------------------------------------------------------------
% State: ready
% -------------------------------------------------------------------------------

handle_event(enter, _OldState, #s_ready{}, ConnectionData) ->
    {keep_state_and_data, [
        {state_timeout, ConnectionData#data.ping_interval, ping}
    ]};

handle_event(state_timeout, ping, #s_ready{status = Status}, ConnectionData) ->
    {next_state, #s_pinging{status = Status}, ConnectionData, [
        {next_event, internal, #send{messages = [
            #pgc_protocol_message:sync{}
        ]}}
    ]};

handle_event(internal, #query{} = Query, #s_ready{}, ConnectionData) ->
    #query{
        text = QueryText
    } = Query,
    pgc_connection_statem_simple_query:enter(QueryText, ConnectionData);

handle_event(internal, #prepare{name = Name, text = Text}, #s_ready{}, ConnectionData) ->
    pgc_connection_statem_extended_query:prepare(Name, Text, ConnectionData);

handle_event(internal, #unprepare{name = Name}, #s_ready{}, ConnectionData) ->
    pgc_connection_statem_extended_query:unprepare(Name, ConnectionData);

handle_event(internal, #execute{name = Name, parameters = Parameters, options = Options}, #s_ready{}, ConnectionData) ->
    pgc_connection_statem_extended_query:execute(Name, Parameters, Options, ConnectionData);

handle_event(internal, #pgc_protocol_message:ready_for_query{status = Status}, #s_ready{}, ConnectionData) ->
    {next_state, #s_ready{status = Status}, ConnectionData};

% -------------------------------------------------------------------------------
% State: pinging
% -------------------------------------------------------------------------------

handle_event(enter, _OldState, #s_pinging{}, ConnectionData) ->
    {keep_state_and_data, [
        {state_timeout, ConnectionData#data.ping_timeout, pang}
    ]};

handle_event(internal, #pgc_protocol_message:ready_for_query{status = Status}, #s_pinging{}, ConnectionData) ->
    {next_state, #s_ready{status = Status}, ConnectionData};

handle_event(internal, #pgc_protocol_message:_{} = Message, #s_pinging{status = Status}, ConnectionData) ->
    % Not the ReadyForQuery we sent Sync for, but *some* message from the
    % server, that alone proves the connection is alive. Go back to ready
    % using the status from before the ping and re-queue the message so
    % it gets the same generic handling.
    {next_state, #s_ready{status = Status}, ConnectionData, [
        {next_event, internal, Message}
    ]};

handle_event(state_timeout, pang, #s_pinging{}, ConnectionData) ->
    pgc_connection_statem_termination:enter(immediate, ping_timeout, ConnectionData);

% -------------------------------------------------------------------------------
% state: *
% -------------------------------------------------------------------------------

handle_event(Type, Content, State, ConnectionData) ->
    pgc_connection_statem_common:handle_event(Type, Content, State, ConnectionData).
