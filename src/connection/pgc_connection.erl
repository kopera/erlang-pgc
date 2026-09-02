-module(pgc_connection).
-on_load(ensure_deps_loaded/0).

-moduledoc """
The root of a PostgreSQL connection's `gen_statem` callback-module stack, and the
behaviour implemented by connection "handler" modules -- the single contract through
which a connection reports every owner-facing event: the startup handshake completing
or failing, `NOTICE`/`NOTIFY` traffic, and (once the query and replication
sub-protocols exist) result rows and replication data.

`pgc_connection` owns everything that has to work no matter which sub-protocol is
currently driving the connection: opening the transport, the ping/keepalive timer, the
owner's lifecycle (linking/monitoring), and delivering every owner-facing event through
a caller-supplied handler module implementing this behaviour. It also owns the two
states that are meaningfully "outside" any sub-protocol: `disconnected` (before the
handshake starts) and `ready` (idle, between sub-protocols).

The startup handshake itself -- sending the `StartupMessage`, authenticating, waiting
for `ReadyForQuery` -- is implemented by `pgc_connection_startup_protocol`, which this
module `push_callback_module`s into right after the transport connects, and which
`pop_callback_module`s back out of once the handshake succeeds.

A handler module's callbacks are invoked synchronously, from inside the connection
process itself, by whichever sub-protocol module is currently driving the connection.
This is deliberate: it is what lets a handler pace the connection -- most importantly a
replication consumer wanting real backpressure over WAL streaming -- simply by taking
its time to return, at the cost of a slow handler stalling the connection (including
its ping timer) until it does.

`handle_row_data/3` and `handle_replication_data/2` are optional: a handler for a plain
query-only connection has no use for the replication one, and vice-versa. Their exact
shape (in particular the `Ack` return, meant to drive flow control) is still
provisional -- pending the query and replication sub-protocol modules that will
actually call them.
""".
-export([
    start_link/3,
    start_link/4,
    stop/1
]).
-export_type([
    start_options/0,
    connection_info/0
]).

-export([
    stop_with_error/2,
    handle_common_event/4
]).
-export_record([
    connection,
    ready,
    send
]).


-behaviour(gen_statem).
-export([
    init/1,
    callback_mode/0,
    handle_event/4,
    terminate/3,
    format_status/1
]).

% ------------------------------------------------------------------------------
% Behaviour
% ------------------------------------------------------------------------------

-type connection_info() :: #{
    % backend_key := {non_neg_integer(), binary()} | undefined,
    parameters := #{binary() => binary()}
}.

-doc "Called once, from inside `c:init/1`'s caller process, with the `Args` from the `handler` start option.".
-callback init(Args :: term()) -> {ok, State :: term()}.

-doc "The startup handshake succeeded and the connection reached `ReadyForQuery` for the first time.".
-callback handle_connected(connection_info(), State) -> {ok, State} when
    State :: term().

-doc "The connection is about to stop. No further callbacks follow.".
-callback terminate(Reason :: term(), State) -> ok when
    State :: term().

-doc "A `NoticeResponse` was received.".
-callback handle_notice(pgc_protocol_message:notice_response_fields(), State) -> {ok, State} when
    State :: term().

-doc "A `NotificationResponse` (the result of some session's `NOTIFY`) was received.".
-callback handle_notification(Channel, Payload, SenderId, State) -> {ok, State} when
    Channel :: binary(),
    Payload :: binary(),
    SenderId :: non_neg_integer(),
    State :: term().

-doc "Provisional -- see the moduledoc. A `DataRow` was received while executing a query.".
-callback handle_row_data(RowDescription :: term(), Row :: [null | binary()], State) ->
      {ok, State}
    | {ok, Ack :: term(), State}
    when State :: term().

-doc "Provisional -- see the moduledoc. A replication message was received while streaming.".
-callback handle_replication_data(Message :: term(), State) ->
      {ok, State}
    | {ok, Ack :: term(), State}
    when State :: term().

-optional_callbacks([
    handle_row_data/3,
    handle_replication_data/2
]).


-define(DEFAULT_PING_INTERVAL, 5000).
-define(DEFAULT_HIBERNATE_AFTER, ?DEFAULT_PING_INTERVAL div 2).
-define(DEFAULT_CONNECT_TIMEOUT, 5000).

% ------------------------------------------------------------------------------
% Types
% ------------------------------------------------------------------------------

-record #start_params{
    address :: pgc_transport:address(),

    connect_options :: pgc_transport:connect_options(),
    connect_timeout :: timeout(),
    ping_interval :: timeout(),
    ping_timeout :: timeout(),

    user :: unicode:chardata(),
    password :: fun(() -> unicode:chardata()),
    database :: unicode:chardata(),
    parameters :: #{
        atom() => unicode:chardata()
    },

    handler_module :: module(),
    handler_args :: term()
}.


% ------------------------------------------------------------------------------
% API
% ------------------------------------------------------------------------------

-doc """
Starts a new connection process, owned by the caller and linked to it.

Given its asynchronous nature, this function returns as soon as the process is
spawned - the actual TCP connect and Postgres handshake happen asynchronously
inside the new process. Every event the connection has to report -- the handshake
succeeding or failing, `NOTICE`/`NOTIFY` traffic, and so on -- is delivered through
the supplied `Module` callback module, implementing this behaviour, invoked
synchronously from inside this process.
""".
-spec start_link(module(), term(), start_options()) -> gen_statem:start_ret().
start_link(Module, Args, Options) ->
    start_link(Module, Args, Options, self()).


-doc """
Starts a new connection process, owned by `Owner` and linked to it.

`Owner` governs only this process's lifecycle: it is monitored, and the connection
stops itself (reporting `normal` to its handler) when `Owner` exits. It plays no part
in event delivery -- that is entirely the `handler` module's job, see `start_link/1`.

> #### Note {: .info }
>
> This function is used by the Pool API to provide an explicit owner.
""".
-spec start_link(module(), term(), start_options(), pid()) -> gen_statem:start_ret().
-type start_options() :: #{
    address := pgc_transport:address(),
    tls => disable | prefer | require,
    tls_options => [ssl:tls_client_option()],
    connect_timeout => timeout(),
    ping_interval => timeout(),

    user := unicode:chardata(),
    password => unicode:chardata() | fun(() -> unicode:chardata()),
    database := unicode:chardata(),
    parameters => #{
        atom() => unicode:chardata()
    },

    handler := module() | {module(), Args :: term()}
}.
start_link(Module, Args, Options, Owner) when is_pid(Owner) ->
    PingInterval = maps:get(ping_interval, Options, ?DEFAULT_PING_INTERVAL),

    %% Start Parameters
    StartParams = #start_params{
        address = maps:get(address, Options),

        connect_options = maps:with([tls, tls_options], Options),
        connect_timeout = maps:get(connect_timeout, Options, ?DEFAULT_CONNECT_TIMEOUT),
        ping_interval = PingInterval,
        ping_timeout = if
            is_integer(PingInterval), PingInterval > 0 ->
                2 * PingInterval;
            PingInterval =:= infinity ->
                infinity
        end,

        user = maps:get(user, Options),
        password = case Options of
            #{password := Password} when is_function(Password, 0) -> Password;
            #{password := Password} -> fun () -> Password end;
            #{} -> fun () -> <<>> end
        end,
        database = maps:get(database, Options),
        parameters = maps:get(parameters, Options, #{}),

        handler_module = Module,
        handler_args = Args
    },
    %% FSM
    HibernateAfter = case PingInterval of
        infinity -> ?DEFAULT_HIBERNATE_AFTER;
        _ -> PingInterval div 2
    end,
    gen_statem:start_link(?MODULE, {Owner, StartParams}, [
        {hibernate_after, HibernateAfter}
    ]).


-doc """
Stops a connection cleanly. The handler's `c:terminate/2` is still invoked, with
reason `normal`.
""".
-spec stop(pid()) -> ok.
stop(Connection) ->
    gen_statem:stop(Connection).


% ------------------------------------------------------------------------------
% States
% ------------------------------------------------------------------------------

-record #disconnected{
}.

-record #ready{
    status :: idle | transaction | error
}.

-record #syncing{
    timeout :: timeout()
}.

-record #stopping{
    reason ::
        normal
        | pgc_transport:error()
        | pgc_auth:error()
        | pgc_protocol:error()
}.


% ------------------------------------------------------------------------------
% Events
% ------------------------------------------------------------------------------

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


% ------------------------------------------------------------------------------
% Data
% ------------------------------------------------------------------------------

-record #connection{
    owner_monitor :: reference(),

    transport :: pgc_transport:t() | undefined,
    transport_buffer :: binary(),

    ping_interval :: timeout(),
    ping_timeout :: timeout(),

    backend_key :: {non_neg_integer(), binary()} | undefined,
    backend_parameters :: #{binary() => binary()},

    types :: pgc_connection_types:t(),

    handler_module :: module(),
    handler_state :: term()
}.

% ------------------------------------------------------------------------------
% gen_statem callbacks
% ------------------------------------------------------------------------------

-doc false.
init({Owner, StartParams}) ->
    OwnerMonitor = erlang:monitor(process, Owner, [
        {tag, {'DOWN', owner}}
    ]),

    #start_params{
        address = Address,
        connect_options = ConnectOptions,
        connect_timeout = ConnectTimeout,
        ping_interval = PingInterval,
        ping_timeout = PingTimeout,

        user = User,
        password = Password,
        database = Database,
        parameters = Parameters,

        handler_module = HandlerModule,
        handler_args = HandlerArgs
    } = StartParams,
    {ok, HandlerState} = HandlerModule:init(HandlerArgs),
    {ok, #disconnected{}, #pgc_connection:connection{
        owner_monitor = OwnerMonitor,

        transport = undefined,
        transport_buffer = <<>>,

        ping_interval = PingInterval,
        ping_timeout = PingTimeout,

        backend_key = undefined,
        backend_parameters = #{},

        types = pgc_connection_types:new(),

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
terminate(Reason, State, #connection{} = Data) ->
    #connection{
        transport = Transport,

        handler_module = Handler,
        handler_state = HandlerState
    } = Data,
    case Transport of
        undefined -> ok;
        _ ->  pgc_transport:close(Transport)
    end,
    case State of
        #stopping{reason = StopReason} ->
            Handler:terminate(StopReason, HandlerState);
        _ ->
            Handler:terminate(Reason, HandlerState)
    end.

-doc false.
format_status(Status) ->
    maps:map(fun
        (data, #connection{} = Data) ->
            Data#connection{types = redacted};
        (_Key, Value) ->
            Value
    end, Status).


% -------------------------------------------------------------------------------
% State: disconnected
% -------------------------------------------------------------------------------

handle_event(enter, _, #disconnected{}, _Data) ->
    keep_state_and_data;

handle_event(internal, #connect{} = Connect, #disconnected{}, Data) ->
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
            {ok, NextState, NextData, Actions} = pgc_connection_startup_protocol:init({
                User,
                Password,
                Database,
                Parameters,
                Data#pgc_connection:connection{transport = Transport}
            }),
            ok = pgc_transport:set_active(Transport, once),
            {next_state, NextState, NextData, [
                {push_callback_module, pgc_connection_startup_protocol} | Actions
            ]};
        {error, ConnectError} ->
            stop_with_error(ConnectError, Data)
    end;


% -------------------------------------------------------------------------------
% State: ready -- idle, between sub-protocols. Owns the ping/keepalive timer.
% -------------------------------------------------------------------------------

handle_event(enter, _OldState, #ready{}, Data) ->
    {keep_state_and_data, [
        {state_timeout, Data#connection.ping_interval, ping}
    ]};

handle_event(state_timeout, ping, #ready{}, Data) ->
    {next_state, #syncing{timeout = Data#connection.ping_timeout}, Data, [
        {next_event, internal, #send{messages = [#pgc_protocol_message:sync{}]}}
    ]};

handle_event(cast, _Request, #ready{}, _Data) ->
    keep_state_and_data;


% -------------------------------------------------------------------------------
% State: syncing -- ping/keepalive: send Sync, expect ReadyForQuery back within
% ping_timeout, otherwise consider the connection dead.
% -------------------------------------------------------------------------------

handle_event(enter, _OldState, #syncing{timeout = Timeout}, _Data) ->
    {keep_state_and_data, [
        {state_timeout, Timeout, pang}
    ]};

handle_event(internal, #pgc_protocol_message:ready_for_query{status = Status}, #syncing{}, Data) ->
    {next_state, #ready{status = Status}, Data};

handle_event(state_timeout, pang, #syncing{}, Data) ->
    stop_with_error(#pgc_transport:error{reason = timeout}, Data);


% -------------------------------------------------------------------------------
% State: stopping
% -------------------------------------------------------------------------------

handle_event(enter, _OldState, #stopping{}, #connection{transport = undefined}) ->
    {stop, normal};

handle_event(enter, _OldState, #stopping{}, #connection{transport = Transport} = Data) when Transport =/= undefined ->
    case pgc_transport:send(Transport, pgc_protocol_messages:encode([#pgc_protocol_message:terminate{}])) of
        ok ->
            {keep_state_and_data, [
                {state_timeout, Data#connection.ping_interval, stop}
            ]};
        _ ->
            {stop, normal}
    end;

handle_event(state_timeout, stop, #stopping{}, _Data) ->
    {stop, normal};

% -------------------------------------------------------------------------------
% state: * -- anything not specific to one of the states above.
% -------------------------------------------------------------------------------

handle_event(Type, Content, State, Data) ->
    handle_common_event(Type, Content, State, Data).


% ------------------------------------------------------------------------------
% Shared helpers for sub-protocol modules
% ------------------------------------------------------------------------------

-doc """
The "hard rule" helper: whenever any sub-protocol module hits a fatal condition, it
hands control back to this module (`change_callback_module`) instead of returning
`{stop, ...}` itself, so `terminate/3` and `format_status/1` always run against this
module regardless of which sub-protocol was active when things went wrong.
""".
-spec stop_with_error(Reason, #connection{}) -> gen_statem:event_handler_result(term()) when
    Reason ::
        pgc_transport:error()
        | pgc_auth:error()
        | pgc_protocol:error().
stop_with_error(Reason, Data) ->
    {next_state, #stopping{reason = Reason}, Data, [
        {change_callback_module, ?MODULE}
    ]}.


-doc """
Handles every event that has to work no matter which sub-protocol module is
currently on top of the callback-module stack: pumping raw transport messages into
decoded protocol messages, folding `ParameterStatus`, delivering `NoticeResponse` and
`NotificationResponse` (`NOTIFY`) to the handler, and reacting to the owner going
down. Every sub-protocol module's `handle_event/4` ends with a fallback clause that
delegates here.
""".
-spec handle_common_event(gen_statem:event_type(), term(), term(), #connection{}) -> gen_statem:event_handler_result(term()).
handle_common_event(info, {{'DOWN', owner}, OwnerMonitor, process, _Pid, _Reason}, _State, #connection{owner_monitor = OwnerMonitor} = Data) ->
    {next_state, #stopping{reason = normal}, Data, [
        {change_callback_module, ?MODULE}
    ]};

handle_common_event(info, Info, State, Data) ->
    case Data#connection.transport of
        undefined ->
            logger:warning("pgc_connection: unexpected message ~w before connecting", [Info]),
            keep_state_and_data;
        Transport ->
            #connection{transport_buffer = Buffer} = Data,
            case pgc_transport:handle_message(Transport, Info) of
                {data, TransportData} ->
                    {Messages, Rest} = pgc_protocol_messages:decode(<<Buffer/binary, TransportData/binary>>),
                    ok = pgc_transport:set_active(Transport, once),
                    {keep_state, Data#connection{transport_buffer = Rest}, [
                        {next_event, internal, Message} || Message <- Messages
                    ]};
                {error, TransportError} ->
                    case State of
                        #stopping{} ->
                            {stop, normal};
                        _ ->
                            stop_with_error(TransportError, Data)
                    end;
                unknown ->
                    logger:warning("pgc_connection: unexpected message ~w", [Info]),
                    keep_state_and_data
            end
    end;

handle_common_event(internal, #pgc_protocol_message:parameter_status{name = Name, value = Value}, _State, Data) ->
    #connection{backend_parameters = BackendParameters} = Data,
    {keep_state, Data#connection{
        backend_parameters = BackendParameters#{Name => Value}
    }};

handle_common_event(internal, #pgc_protocol_message:notice_response{fields = Fields}, _State, Data) ->
    #connection{
        handler_module = Module,
        handler_state = HandlerState0
    } = Data,
    {ok, HandlerState1} = Module:handle_notice(Fields, HandlerState0),
    {keep_state, Data#connection{handler_state = HandlerState1}};

handle_common_event(internal, #pgc_protocol_message:notification_response{id = SenderId, channel = Channel, payload = Payload}, _State, Data) ->
    #connection{
        handler_module = Module,
        handler_state = HandlerState0
    } = Data,
    {ok, HandlerState1} = Module:handle_notification(Channel, Payload, SenderId, HandlerState0),
    {keep_state, Data#connection{handler_state = HandlerState1}};

handle_common_event(internal, #send{}, _State, #connection{transport = undefined}) ->
    keep_state_and_data;
handle_common_event(internal, #send{messages = Messages}, _State, #connection{transport = Transport} = Data) when Transport =/= undefined ->
    case pgc_transport:send(Transport, pgc_protocol_messages:encode(Messages)) of
        ok ->
            keep_state_and_data;
        {error, Error} ->
            {next_state, #stopping{reason = Error}, Data#connection{transport = undefined}}
    end.


% ------------------------------------------------------------------------------
% Internals
% ------------------------------------------------------------------------------

ensure_deps_loaded() ->
    {module, _} = code:ensure_loaded(pgc_protocol_message),
    ok.
