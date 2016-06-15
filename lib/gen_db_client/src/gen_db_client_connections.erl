-module(gen_db_client_connections).
-export([
    checkout/1,
    checkout/2,
    checkin/2
]).

-export([
    start_link/2,
    start_link/3
]).

-behaviour(gen_server).
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-record(connection, {
    pid = self() :: pid(),
    monitor = make_ref() :: reference(),
    req :: reference() | undefined,
    user_pid :: pid() | undefined,
    user_monitor :: reference() | undefined
}).
-record(state, {
    connections_sup :: pid(),
    used = [] :: [#connection{}],
    available = [] :: [#connection{}],
    waiting = queue:new() :: queue:queue({Req :: reference(), ReplyTo :: {pid(), reference()}}),
    size = 0 :: non_neg_integer(),
    limit :: pos_integer()
}).

-spec start_link(pid(), start_options()) -> {ok, connections()}.
-type start_options() :: #{limit => pos_integer()}.
-type connections() :: pid().
start_link(Supervisor, Opts) ->
    proc_lib:start_link(?MODULE, init, [[self(), Supervisor, Opts]]).

start_link(Supervisor, Name, Opts) ->
    proc_lib:start_link(?MODULE, init, [[self(), Supervisor, Name, Opts]]).


-spec checkout(connections()) -> {ok, pid()} | {error, timeout}.
checkout(Connections) ->
    checkout(Connections, 5000).

-spec checkout(connections(), timeout()) -> {ok, pid()} | {error, timeout}.
checkout(Connections, Timeout) ->
    Ref = make_ref(),
    try gen_server:call(Connections, {checkout, Ref}, Timeout) of
        {ok, _} = Result -> Result
    catch
        exit:{timeout, {gen_server, call, _}} ->
            gen_server:cast(Connections, {cancel_checkout, Ref}),
            {error, timeout};
        Class:Reason ->
            gen_server:cast(Connections, {cancel_checkout, Ref}),
            erlang:raise(Class, Reason, erlang:get_stacktrace())
    end.

-spec checkin(connections(), pid()) -> ok.
checkin(Connections, Connection) when is_pid(Connection) ->
    gen_server:cast(Connections, {checkin, Connection}).


%% @hidden
init([Parent, Supervisor, Opts]) ->
    ok = proc_lib:init_ack(Parent, {ok, self()}),
    gen_server:enter_loop(?MODULE, [], #state{
        connections_sup = connections_sup(Supervisor),
        limit = maps:get(limit, Opts, 10)
    });
init([Parent, Supervisor, Name, Opts]) ->
    case register_name(Name) of
        true ->
            init([Parent, Supervisor, Opts]);
        {false, Pid} ->
            proc_lib:init_ack(Parent, {error, {already_started, Pid}})
    end.

connections_sup(Supervisor) ->
    Children = supervisor:which_children(Supervisor),
    [ConnectionSup] = [Pid || {Id, Pid, _, _} <- Children, Id =:= connections_sup],
    ConnectionSup.

register_name({local, Name}) ->
    try register(Name, self()) of
        true -> true
    catch
        error:_ ->
            {false, whereis(Name)}
    end;
register_name({global, Name}) ->
    case global:register_name(Name, self()) of
        yes -> true;
        no -> {false, global:whereis_name(Name)}
    end;
register_name({via, Module, Name}) ->
    case Module:register_name(Name, self()) of
        yes ->
            true;
        no ->
            {false, Module:whereis_name(Name)}
    end.

%% @hidden
handle_call({checkout, Req}, {FromPid, _} = From, State) ->
    #state{
        connections_sup = Supervisor,
        used = Used,
        available = Available,
        size = Size,
        limit = Limit
    } = State,
    case Available of
        [Connection | Left] ->
            {reply, {ok, Connection#connection.pid}, State#state{
                used = [checkout_connection(Req, FromPid, Connection) | Used],
                available = Left
            }};
        [] when Size < Limit ->
            {ok, Connection} = start_connection(Supervisor),
            {reply, {ok, Connection#connection.pid}, State#state{
                used = [checkout_connection(Req, FromPid, Connection) | Used],
                size = Size + 1
            }};
        [] ->
            {noreply, State#state{
                waiting = queue:in({Req, From}, State#state.waiting)
            }}
    end;
handle_call(_Request, _From, State) ->
    {reply, {error, invalid_request}, State}.

%% @hidden
handle_cast({checkin, Pid}, State) ->
    case lists:keytake(Pid, #connection.pid, State#state.used) of
        {value, Connection, Used} ->
            {noreply, handle_waiting(State#state{
                used = Used,
                available = [checkin_connection(Connection) | State#state.available]
            })};
        false ->
            {noreply, State}
    end;
handle_cast({cancel_checkout, Req}, State) ->
    %% we need to handle the case where the cancel is processed after we have marked the connection as used
    case lists:keytake(Req, #connection.req, State#state.used) of
        {value, Connection, StillUsed} ->
            {noreply, handle_waiting(State#state{
                used = StillUsed,
                available = [checkin_connection(Connection) | State#state.available]
            })};
        false ->
            {noreply, State#state{
                waiting = queue:filter(fun ({R, _}) -> R =/= Req end, State#state.waiting)
            }}
    end;
handle_cast(_Request, State) ->
    {noreply, State}.

%% @hidden
handle_info({'DOWN', Monitor, process, _Pid, _}, State) ->
    case lists:keytake(Monitor, #connection.user_monitor, State#state.used) of
        {value, Connection, StillUsed} ->
            {noreply, handle_waiting(State#state{
                used = StillUsed,
                available = [checkin_connection(Connection) | State#state.available]
            })};
        false ->
            %% We need to handle waiting because the size of the pool might have changed
            Used = lists:keydelete(Monitor, #connection.monitor, State#state.used),
            Available = lists:keydelete(Monitor, #connection.monitor, State#state.available),
            {noreply, handle_waiting(State#state{
                used = Used,
                available = Available,
                size = length(Used) + length(Available)
            })}
    end;

%% @hidden
handle_info(_Info, State) ->
    {noreply, State}.

%% @hidden
terminate(_Reason, _State) ->
    ok.

%% @hidden
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


start_connection(Supervisor) ->
    {ok, Pid} = supervisor:start_child(Supervisor, []),
    {ok, #connection{
        pid = Pid,
        monitor = erlang:monitor(process, Pid)
    }}.

checkout_connection(Req, UserPid, #connection{pid = Pid, user_pid = undefined, user_monitor = undefined} = Connection) ->
    gen_db_client_connections:checkout(Pid, UserPid),
    Connection#connection{
        req = Req,
        user_pid = UserPid,
        user_monitor = erlang:monitor(process, UserPid)
    }.

checkin_connection(#connection{pid = Pid, user_pid = UserPid, user_monitor = UserMonitor} = Connection)
        when is_pid(UserPid), is_reference(UserMonitor) ->
    gen_db_client_connections:checkin(Pid, UserPid),
    erlang:demonitor(UserMonitor, [flush]),
    Connection#connection{
        req = undefined,
        user_pid = undefined,
        user_monitor = undefined
    }.

handle_waiting(#state{used = Used, available = [Connection | Available]} = State) ->
    case queue:out(State#state.waiting) of
        {{value, {Req, {UserPid, _} = ReplyTo}}, Waiting} ->
            gen_server:reply(ReplyTo, {ok, Connection#connection.pid}),
            State#state{
                used = [checkout_connection(Req, UserPid, Connection) | Used],
                available = Available,
                waiting = Waiting
            };
        {empty, _} ->
            State
    end;
handle_waiting(#state{connections_sup = Sup, used = Used, available = [], size = Size, limit = Limit} = State)
        when Size < Limit ->
    case queue:out(State#state.waiting) of
        {{value, {Req, {UserPid, _} = ReplyTo}}, Waiting} ->
            {ok, Connection} = start_connection(Sup),
            gen_server:reply(ReplyTo, {ok, Connection#connection.pid}),
            State#state{
                used = [checkout_connection(Req, UserPid, Connection) | Used],
                waiting = Waiting,
                size = Size + 1
            };
        {empty, _} ->
            State
    end;
handle_waiting(#state{size = Size, limit = Limit} = State) when Size =:= Limit ->
    State.
