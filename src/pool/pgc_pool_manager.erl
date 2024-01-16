%% @private
-module(pgc_pool_manager).
-export([
    checkout/2,
    checkin/2
]).

-export([
    start_link/2,
    stop/1
]).

-behaviour(gen_server).
-export([
    init/1,
    handle_continue/2,
    handle_call/3,
    handle_cast/2,
    handle_info/2
    %handle_continue/2,
    %code_change/3,
    %format_status/2,
    %terminate/2
]).

-record(state, {
    supervisor :: pid(),
    connections_supervisor :: pid(),

    starting :: [connection()],
    available :: [connection()],
    used :: [connection()],
    waiting :: queue:queue(checkout_req()),

    size :: non_neg_integer(),
    limit :: pos_integer()
}).

-record(connection, {
    pid :: pid(),
    monitor :: reference(),
    user_pid :: pid() | undefined,
    user_monitor :: reference() | undefined,
    user_reference :: reference() | undefined,
    last_used :: integer() | undefined %% erlang:monotonic_time()
}).
-type connection() :: #connection{}.

-record(checkout_req, {
    user_pid :: pid(),
    user_reference :: reference(),
    user_reply_to :: gen_server:from()
}).
-type checkout_req() :: #checkout_req{}.


-spec checkout(PoolRef, Options) -> {ok, Connection} | {error, timeout} when
    PoolRef :: atom() | pid(),
    Options :: #{
        timeout => timeout()
    },
    Connection :: pid().
checkout(PoolRef, Options) ->
    CheckoutRef = make_ref(),
    Timeout = maps:get(timeout, Options, 5000),
    try gen_server:call(PoolRef, {checkout, self(), CheckoutRef}, Timeout) of
        {ok, _Connection} = Result ->
            Result
    catch
        exit:{timeout, {gen_server, call, _}} ->
            gen_server:cast(PoolRef, {checkout_cancel, self(), CheckoutRef}),
            {error, timeout};
        Class:Reason:Stacktrace ->
            gen_server:cast(PoolRef, {checkout_cancel, self(), CheckoutRef}),
            erlang:raise(Class, Reason, Stacktrace)
    end.


-spec checkin(PoolRef, Connection) -> ok when
    PoolRef :: atom() | pid(),
    Connection :: pid().
checkin(PoolRef, ConnectionPid) when is_pid(ConnectionPid) ->
    gen_server:cast(PoolRef, {checkin, ConnectionPid}).


-spec start_link(PoolSupervisor, PoolOptions) -> {ok, pid()} | {error, term()} when
    PoolSupervisor :: pid(),
    PoolOptions :: pgc_pool:options().
start_link(PoolSupervisor, PoolOptions) ->
    case PoolOptions of
        #{name := PoolName} ->
            gen_server:start_link({local, PoolName}, ?MODULE, {PoolSupervisor, PoolOptions}, []);
        #{} ->
            gen_server:start_link(?MODULE, {PoolSupervisor, PoolOptions}, [])
    end.


stop(PoolRef) ->
    gen_server:stop(PoolRef).


% ------------------------------------------------------------------------------
% gen_server callbacks
% ------------------------------------------------------------------------------

%% @hidden
init(Args) ->
    {ok, undefined, {continue, Args}}.


%% @hidden
handle_continue({PoolSupervisor, PoolOptions}, undefined) ->
    {noreply, #state{
        supervisor = PoolSupervisor,
        connections_supervisor = connections_supervisor(PoolSupervisor),
        starting = [],
        available = [],
        used = [],
        waiting = queue:new(),
        size = 0,
        limit = maps:get(limit, PoolOptions, 1)
    }}.


%% @hidden
handle_call({checkout, UserPid, CheckoutRef}, From, #state{} = State) ->
    CheckoutReq = #checkout_req{
        user_pid = UserPid,
        user_reference = CheckoutRef,
        user_reply_to = From
    },
    {noreply, process_waiting(State#state{
        waiting = queue:in(CheckoutReq, State#state.waiting)
    })};

handle_call(Request, _From, #state{} = State) ->
    {reply, {bad_request, Request}, State}.


%% @hidden
handle_cast({checkin, ConnectionPid}, #state{} = State) when is_pid(ConnectionPid) ->
    {noreply, process_checkin(ConnectionPid, State)};

handle_cast({checkout_cancel, UserPid, CheckoutRef}, #state{} = State) ->
    %% we need to handle the case where the cancel is processed after we have marked the connection as used
    case lists:keyfind(CheckoutRef, #connection.user_reference, State#state.used) of
        #connection{pid = ConnectionPid, user_pid = UserPid} ->
            {noreply, process_checkin(ConnectionPid, State)};
        false ->
            StillWaiting = queue:delete_with(fun (#checkout_req{} = CheckoutReq) ->
                CheckoutRef =:= CheckoutReq#checkout_req.user_reference
                    andalso UserPid =:= CheckoutReq#checkout_req.user_pid
            end, State#state.waiting),
            {noreply, State#state{
                waiting = StillWaiting
            }}
    end;

handle_cast(_Notification, #state{} = State) ->
    {noreply, State}.


%% @hidden
handle_info({{'DOWN', user}, UserMonitor, process, UserPid, _}, #state{} = State) ->
    case lists:keyfind(UserMonitor, #connection.user_monitor, State#state.used) of
        #connection{pid = ConnectionPid, user_pid = UserPid} ->
            {noreply, process_checkin(ConnectionPid, State)};
        false ->
            {noreply, State}
    end;

handle_info({{'DOWN', connection}, ConnectionMonitor, process, _ConnectionPid, _}, #state{} = State) ->
    StillStarting = lists:keydelete(ConnectionMonitor, #connection.monitor, State#state.starting),
    StillAvailable = lists:keydelete(ConnectionMonitor, #connection.monitor, State#state.available),
    StillUsed = lists:keydelete(ConnectionMonitor, #connection.monitor, State#state.used),
    % We need to handle waiting because the size of the pool might have changed: this
    % might allow us to try starting new connections when size < limit and thus
    % servicing pending requests.
    {noreply, process_waiting(State#state{
        starting = StillStarting,
        used = StillUsed,
        available = StillAvailable,
        size = length(StillStarting) + length(StillAvailable) + length(StillUsed)
    })};

handle_info({pgc_connection, ConnectionPid, connected}, #state{} = State) ->
    case lists:keytake(ConnectionPid, #connection.pid, State#state.starting) of
        {value, Connection, StillStarting} ->
            {noreply, process_waiting(State#state{
                starting = StillStarting,
                available = [Connection | State#state.available]
            })};
        false ->
            {noreply, State}
    end;

handle_info({pgc_connection, ConnectionPid, {error, _} = Error}, #state{} = State) ->
    case lists:keytake(ConnectionPid, #connection.pid, State#state.starting) of
        {value, #connection{monitor = ConnectionMonitor}, StillStarting} ->
            _ = erlang:demonitor(ConnectionMonitor, [flush]),
            {noreply, process_waiting(State#state{
                starting = StillStarting,
                waiting = case queue:out(State#state.waiting) of
                    {{value, #checkout_req{user_reply_to = ReplyTo}}, StillWaiting} ->
                        gen_server:reply(ReplyTo, Error),
                        StillWaiting;
                    {empty, StillWaiting} ->
                        StillWaiting
                end,
                size = State#state.size - 1
            })};
        false ->
            {noreply, State}
    end;

handle_info(_Info, #state{} = State) ->
    {noreply, State}.


% ------------------------------------------------------------------------------
% State Helpers
% ------------------------------------------------------------------------------

%% @private
-spec process_checkin(pid(), #state{}) -> #state{}.
process_checkin(ConnectionPid, #state{} = State) ->
    #state{
        available = Available,
        used = Used
    } = State,
    case lists:keytake(ConnectionPid, #connection.pid, Used) of
        {value, Connection, StillUsed} ->
            process_waiting(State#state{
                used = StillUsed,
                available = [connection_checkin(Connection) | Available]
            });
        false ->
            State
    end.

%% @private
-spec process_waiting(#state{}) -> #state{}.
process_waiting(#state{available = [Connection | StillAvailable]} = State) ->
    case queue:out(State#state.waiting) of
        {{value, #checkout_req{user_pid = UserPid, user_reference = CheckoutRef, user_reply_to = ReplyTo}}, StillWaiting} ->
            gen_server:reply(ReplyTo, {ok, Connection#connection.pid}),
            State#state{
                used = [connection_checkout(UserPid, CheckoutRef, Connection) | State#state.used],
                available = StillAvailable,
                waiting = StillWaiting
            };
        {empty, _} ->
            State
    end;
process_waiting(#state{available = [], size = Size, limit = Limit} = State) when Size < Limit ->
    case queue:is_empty(State#state.waiting) of
        false ->
            Connection = connection_start(State#state.connections_supervisor),
            State#state{
                starting = [Connection | State#state.starting],
                size = Size + 1
            };
        true ->
            State
    end;
process_waiting(#state{size = Size, limit = Limit} = State) when Size =:= Limit ->
    State.


% ------------------------------------------------------------------------------
% Helpers
% ------------------------------------------------------------------------------

-spec connections_supervisor(pid()) -> pid().
connections_supervisor(PoolSupervisor) ->
    Children = supervisor:which_children(PoolSupervisor),
    [ConnectionSup] = [Pid || {connections_sup, Pid, _, _} <- Children],
    ConnectionSup.


-spec connection_start(pid()) -> connection().
connection_start(ConnectionsSup) ->
    {ok, ConnectionPid} = pgc_pool_connections_sup:start_connection(ConnectionsSup),
    #connection{
        pid = ConnectionPid,
        monitor = erlang:monitor(process, ConnectionPid, [
            {tag, {'DOWN', connection}}
        ])
    }.


-spec connection_checkout(pid(), term(), connection()) -> connection().
connection_checkout(UserPid, CheckoutRef, #connection{user_pid = undefined, user_monitor = undefined} = Connection) ->
    Connection#connection{
        user_pid = UserPid,
        user_monitor = erlang:monitor(process, UserPid, [
            {tag, {'DOWN', user}}
        ]),
        user_reference = CheckoutRef,
        last_used = erlang:monotonic_time()
    }.


-spec connection_checkin(connection()) -> connection().
connection_checkin(#connection{user_pid = UserPid, user_monitor = UserMonitor} = Connection) when is_pid(UserPid), is_reference(UserMonitor) ->
    % pgc_connection:reset(ConnectionPid),
    _ = erlang:demonitor(UserMonitor, [flush]),
    Connection#connection{
        user_pid = undefined,
        user_monitor = undefined,
        user_reference = undefined,
        last_used = erlang:monotonic_time()
    }.