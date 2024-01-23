%% @private
-module(pgc_pool_manager).
-export([
    info/1,
    checkout/2,
    checkin/2
]).

-export([
    start_link/3,
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

-include_lib("kernel/include/logger.hrl").


-record(state, {
    owner :: {pid(), reference()} | undefined,
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



-spec info(PoolManagerRef) -> Info when
    PoolManagerRef :: atom() | pid(),
    Info :: #{
        starting := non_neg_integer(),
        available := non_neg_integer(),
        used := non_neg_integer(),
        size := non_neg_integer(),
        limit := pos_integer()
    }.
info(PoolManagerRef) ->
    gen_server:call(PoolManagerRef, info).


-spec checkout(PoolManagerRef, Options) -> {ok, Connection} | {error, timeout} when
    PoolManagerRef :: atom() | pid(),
    Options :: #{
        timeout => timeout() | {abs, number()}
    },
    Connection :: pid().
checkout(PoolManagerRef, Options) ->
    Timeout = maps:get(timeout, Options, infinity),
    ReqId = gen_server:send_request(PoolManagerRef, checkout),
    try gen_server:receive_response(ReqId, Timeout) of
        {reply, {ok, Connection} = Result} when is_pid(Connection) ->
            Result;
        {error, _} ->
            exit(noproc);
        timeout ->
            gen_server:cast(PoolManagerRef, {checkout_cancel, {self(), ReqId}}),
            {error, timeout}
    catch
        Class:Reason:Stacktrace ->
            gen_server:cast(PoolManagerRef, {checkout_cancel, {self(), ReqId}}),
            erlang:raise(Class, Reason, Stacktrace)
    end.


-spec checkin(PoolManagerRef, Connection) -> ok when
    PoolManagerRef :: atom() | pid(),
    Connection :: pid().
checkin(PoolManagerRef, ConnectionPid) when is_pid(ConnectionPid) ->
    gen_server:cast(PoolManagerRef, {checkin, ConnectionPid}).


-spec start_link(PoolOwner, PoolSupervisor, PoolOptions) -> {ok, pid()}  when
    PoolOwner :: pid() | undefined,
    PoolSupervisor :: pid(),
    PoolOptions :: pgc_pool:options().
start_link(PoolOwner, PoolSupervisor, PoolOptions) ->
    Args = {PoolOwner, PoolSupervisor, PoolOptions},
    {ok, _} = case PoolOptions of
        #{name := PoolName} ->
            gen_server:start_link({local, PoolName}, ?MODULE, Args, []);
        #{} ->
            gen_server:start_link(?MODULE, Args, [])
    end.


stop(PoolRef) ->
    gen_server:stop(PoolRef).


% ------------------------------------------------------------------------------
% gen_server callbacks
% ------------------------------------------------------------------------------

%% @private
init(Args) ->
    {ok, undefined, {continue, Args}}.


%% @private
handle_continue({OwnerPid, SupervisorPid, Options}, undefined) ->
    {noreply, #state{
        owner = if
            OwnerPid =:= undefined ->
                undefined;
            is_pid(OwnerPid) ->
                OwnerMonitor = erlang:monitor(process, OwnerPid, [
                    {tag, {'DOWN', owner}}
                ]),
                {OwnerPid, OwnerMonitor}
        end,
        supervisor = SupervisorPid,
        connections_supervisor = connections_supervisor(SupervisorPid),
        starting = [],
        available = [],
        used = [],
        waiting = queue:new(),
        size = 0,
        limit = maps:get(limit, Options, 1)
    }}.


%% @private
handle_call(info, _From, #state{} = State) ->
    Info = #{
        starting => length(State#state.starting),
        available => length(State#state.available),
        used => length(State#state.used),
        size => State#state.size,
        limit => State#state.limit
    },
    {reply, Info, State};
handle_call(checkout, {UserPid, CheckoutRef} = From, #state{} = State) ->
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


%% @private
handle_cast({checkin, ConnectionPid}, #state{} = State) when is_pid(ConnectionPid) ->
    {noreply, process_checkin(ConnectionPid, State)};

handle_cast({checkout_cancel, {UserPid, CheckoutRef}}, #state{} = State) ->
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
                % eqwalizer:ignore
                waiting = StillWaiting
            }}
    end;

handle_cast(EventContent, #state{} = State) ->
    ?LOG_WARNING(#{
        label => {?MODULE, unhandled_event},
        event_type => cast,
        event_content => EventContent
    }),
    {noreply, State}.


%% @private
handle_info({{'DOWN', owner}, OwnerMonitor, process, OwnerPid, _}, #state{owner = {OwnerPid, OwnerMonitor}} = State) ->
    {stop, normal, State};

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

handle_info(EventContent, #state{} = State) ->
    ?LOG_WARNING(#{
        label => {?MODULE, unhandled_event},
        event_type => info,
        event_content => EventContent
    }),
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
    [ConnectionSup] = [Pid || {connections_sup, Pid, _, _} <- Children, is_pid(Pid)],
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


-spec connection_checkout(pid(), reference(), connection()) -> connection().
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
    pgc_connection:reset(Connection#connection.pid),
    _ = erlang:demonitor(UserMonitor, [flush]),
    Connection#connection{
        user_pid = undefined,
        user_monitor = undefined,
        user_reference = undefined,
        last_used = erlang:monotonic_time()
    }.