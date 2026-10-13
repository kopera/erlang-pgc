-module(pgc_pool_manager).
-moduledoc false.
-export([
    start_link/2
]).
-export([
    info/1,
    checkout/2,
    checkin/2
]).
-export_type([
    checkout_options/0
]).

-behaviour(gen_server).
-export([
    init/1,
    handle_continue/2,
    handle_call/3,
    handle_cast/2,
    handle_info/2
]).

-record #state{
    client_sup :: pid(),

    available :: [client_ref()],
    used :: #{client_ref() => checkout()},
    waiting :: queue:queue({gen_server:from(), checkout()}),

    size :: non_neg_integer(),
    max_size :: pos_integer()
}.

-type client_ref() :: pid().

-record #checkout{
    id :: reference(),

    user_pid :: pid(),
    user_monitor :: reference()
}.
-type checkout() :: #checkout{}.

% ------------------------------------------------------------------------------
% API
% ------------------------------------------------------------------------------


-spec start_link(SupervisorPid, PoolOptions) -> gen_server:start_ret() when
    SupervisorPid :: pid(),
    PoolOptions :: pgc_pool:options().
start_link(SupervisorPid, PoolOptions) ->
    Args = {SupervisorPid, PoolOptions},
    gen_server:start_link(?MODULE, Args, []).


-spec info(ManagerRef) -> Info when
    ManagerRef :: pid(),
    Info :: pgc_pool:info().
info(ManagerRef) ->
    gen_server:call(ManagerRef, info).


-doc """
Checks a connection out, or waits for one to free up / be started, up to `Options`'s `timeout`.
""".
-spec checkout(ManagerRef, Options) -> {ok, Client} | {error, timeout} when
    ManagerRef :: pid(),
    Options :: #{timeout => timeout() | {abs, integer()}},
    Client :: pid().
-type checkout_options() :: #{
    timeout => timeout() | {abs, integer()}
}.
checkout(ManagerRef, Options) ->
    Timeout = maps:get(timeout, Options, infinity),
    CheckoutId = make_ref(),
    ReqId = gen_server:send_request(ManagerRef, {checkout, CheckoutId}),
    try gen_server:receive_response(ReqId, Timeout) of
        {reply, {ok, Connection} = Result} when is_pid(Connection) ->
            Result;
        {error, _} ->
            exit(noproc);
        timeout ->
            gen_server:cast(ManagerRef, {checkout_cancel, CheckoutId}),
            {error, timeout}
    catch
        Class:Reason:Stacktrace ->
            gen_server:cast(ManagerRef, {checkout_cancel, CheckoutId}),
            erlang:raise(Class, Reason, Stacktrace)
    end.


-spec checkin(ManagerRef, Connection) -> ok when
    ManagerRef :: pid(),
    Connection :: pid().
checkin(ManagerRef, ConnectionPid) when is_pid(ConnectionPid) ->
    gen_server:cast(ManagerRef, {checkin, ConnectionPid}).


% ------------------------------------------------------------------------------
% gen_server callbacks
% ------------------------------------------------------------------------------

-doc false.
init(Args) ->
    {ok, undefined, {continue, Args}}.


-doc """
Deferred from `init/1`: at that point the supervisor named by `SupervisorPid` is still
synchronously working through its own child-start list (this manager is one of the children it's
starting), so it can't yet answer a `which_children` call made from inside that same call chain.
Deferring the lookup via `{continue, ...}` lets `start_link/2` return first.
""".
handle_continue({SupervisorPid, PoolOptions}, undefined) ->
    {noreply, #state{
        client_sup = client_sup(SupervisorPid),

        available = [],
        used = #{},
        waiting = queue:new(),

        size = 0,
        max_size = maps:get(max_size, PoolOptions, 1)
    }}.


-doc false.
handle_call(info, _From, #state{} = State) ->
    Info = #{
        available => length(State#state.available),
        waiting => queue:len(State#state.waiting),
        used => maps:size(State#state.used),
        size => State#state.size,
        max_size => State#state.max_size
    },
    {reply, Info, State};

handle_call({checkout, CheckoutId}, {UserPid, _} = From, #state{} = State) ->
    #state{
        waiting = Waiting
    } = State,
    {noreply, process_waiting(State#state{
        waiting = queue:in({From, #checkout{
            id = CheckoutId,

            user_pid = UserPid,
            user_monitor = erlang:monitor(process, UserPid, [{tag, {'DOWN', user}}])
        }}, Waiting)
    })}.


-doc false.
handle_cast({checkin, ConnectionPid}, #state{} = State) ->
    {noreply, process_checkin(ConnectionPid, State)};

handle_cast({checkout_cancel, CheckoutId}, #state{used = Used} = State) ->
    % The cancel may be processed after the checkout was already granted -- handle both.
    case find_used(fun (Checkout) -> Checkout#checkout.id =:= CheckoutId end, Used) of
        {ok, ConnectionPid} ->
            {noreply, process_checkin(ConnectionPid, State)};
        error ->
            {noreply, State#state{
                waiting = queue:filter(fun ({_From, Checkout}) ->
                    case Checkout of
                        #checkout{id = CheckoutId, user_monitor = UserMonitor} ->
                            erlang:demonitor(UserMonitor, [flush]),
                            false;
                        #checkout{} ->
                            true
                    end
                end, State#state.waiting)
            }}
    end.


-doc false.
handle_info({{'DOWN', connection}, _ConnectionMonitor, process, ConnectionPid, _Reason}, #state{} = State) ->
    {noreply, process_waiting(remove_client(ConnectionPid, State))};

handle_info({{'DOWN', user}, UserMonitor, process, _UserPid, _Reason}, #state{used = Used} = State) ->
    case find_used(fun (Checkout) -> Checkout#checkout.user_monitor =:= UserMonitor end, Used) of
        {ok, ConnectionPid} ->
            {noreply, process_checkin(ConnectionPid, State)};
        error ->
            {noreply, State#state{
                waiting = queue:filter(fun ({_From, Checkout}) ->
                    Checkout#checkout.user_monitor =/= UserMonitor
                end, State#state.waiting)
            }}
    end.


% ------------------------------------------------------------------------------
% Helpers
% ------------------------------------------------------------------------------

-spec process_waiting(#state{}) -> #state{}.
process_waiting(#state{available = [ConnectionPid | RestAvailable], waiting = Waiting, used = Used} = State) ->
    case queue:out(Waiting) of
        {{value, {ReplyTo, Checkout}}, RestWaiting} ->
            gen_server:reply(ReplyTo, {ok, ConnectionPid}),
            process_waiting(State#state{
                available = RestAvailable,
                used = Used#{
                    ConnectionPid => Checkout
                },
                waiting = RestWaiting
            });
        {empty, _} ->
            State
    end;

process_waiting(#state{available = [], size = Size, max_size = Limit, waiting = Waiting} = State) when Size < Limit ->
    case queue:is_empty(Waiting) of
        false ->
            {ok, ConnectionPid} = pgc_pool_client_sup:start_connection(State#state.client_sup),
            _ = erlang:monitor(process, ConnectionPid, [{tag, {'DOWN', connection}}]),
            process_waiting(State#state{available = [ConnectionPid], size = Size + 1});
        true ->
            State
    end;

process_waiting(#state{} = State) ->
    State.


-spec process_checkin(pid(), #state{}) -> #state{}.
process_checkin(ClientPid, #state{used = Used, available = Available} = State) ->
    case maps:take(ClientPid, Used) of
        {#checkout{user_pid = _UserPid, user_monitor = UserMonitor}, RestUsed} ->
            erlang:demonitor(UserMonitor, [flush]),
            case reset_client(ClientPid) of
                ok ->
                    process_waiting(State#state{used = RestUsed, available = [ClientPid | Available]});
                error ->
                    State#state{used = RestUsed}
            end;
        error ->
            State
    end.


-spec remove_client(pid(), #state{}) -> #state{}.
remove_client(ClientPid, #state{used = Used, available = Available, size = Size} = State) ->
    case maps:take(ClientPid, Used) of
        {#checkout{user_monitor = UserMonitor}, RestUsed} ->
            erlang:demonitor(UserMonitor, [flush]),
            State#state{used = RestUsed, size = Size - 1};
        error ->
            State#state{available = lists:delete(ClientPid, Available), size = Size - 1}
    end.


-spec reset_client(pid()) -> ok | error.
reset_client(ClientPid) ->
    try pgc_client:reset(ClientPid) of
        ok -> ok
    catch
        exit:_ -> error
    end.


-spec find_used(fun((V) -> boolean()), #{K => V}) -> {ok, K} | error.
find_used(Pred, Map) ->
    case lists:search(fun ({_K, V}) -> Pred(V) end, maps:to_list(Map)) of
        {value, {K, _V}} -> {ok, K};
        false -> error
    end.


-spec client_sup(pid()) -> pid().
client_sup(SupervisorPid) ->
    case supervisor:which_child(SupervisorPid, client_sup) of
        {ok, {_, ClientSup, _, _}} when is_pid(ClientSup) ->
            ClientSup
    end.


-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

test_state(Available, Used, Waiting) ->
    #state{
        client_sup = self(),
        available = Available,
        used = Used,
        waiting = Waiting,
        size = length(Available) + map_size(Used),
        max_size = length(Available) + map_size(Used)
    }.

standin() ->
    spawn(fun () -> receive stop -> ok end end).

dead_standin() ->
    Pid = standin(),
    Ref = erlang:monitor(process, Pid),
    Pid ! stop,
    receive {'DOWN', Ref, process, Pid, _} -> Pid end.

-doc "An available connection is handed straight to the head of the waiting queue.".
process_waiting_hands_out_available_connection_test() ->
    ConnectionPid = standin(),
    UserPid = standin(),
    UserMonitor = erlang:monitor(process, UserPid),
    CheckoutId = make_ref(),
    From = {self(), make_ref()},
    Waiting = queue:in({From, #checkout{id = CheckoutId, user_pid = UserPid, user_monitor = UserMonitor}}, queue:new()),
    NewState = process_waiting(test_state([ConnectionPid], #{}, Waiting)),
    ?assertEqual([], NewState#state.available),
    ?assertMatch(#{ConnectionPid := #checkout{id = CheckoutId}}, NewState#state.used),
    {_, Tag} = From,
    ?assertEqual({Tag, {ok, ConnectionPid}}, receive Message -> Message after 0 -> timeout end),
    erlang:demonitor(UserMonitor, [flush]),
    ConnectionPid ! stop,
    UserPid ! stop.

-doc "Nothing to hand out, nothing changes.".
process_waiting_noop_when_waiting_empty_test() ->
    ConnectionPid = standin(),
    State = test_state([ConnectionPid], #{}, queue:new()),
    ?assertEqual(State, process_waiting(State)),
    ConnectionPid ! stop.

-doc "At capacity, with no available connection: waiting queue is left untouched.".
process_waiting_noop_at_capacity_test() ->
    UserPid = standin(),
    UserMonitor = erlang:monitor(process, UserPid),
    Waiting = queue:in({{self(), make_ref()}, #checkout{id = make_ref(), user_pid = UserPid, user_monitor = UserMonitor}}, queue:new()),
    State = (test_state([], #{}, Waiting))#state{size = 1, max_size = 1},
    ?assertEqual(State, process_waiting(State)),
    erlang:demonitor(UserMonitor, [flush]),
    UserPid ! stop.

-doc """
A connection that's already dead by the time it's checked in (it doesn't have to still be alive
to reset -- `reset_client/1` covers that) is dropped instead of handed to the next waiter, and
doesn't take the manager down with it. Its former user is still demonitored; a live `pgc_client`
actually being reset successfully is exercised at the integration level (`pgc_pool_SUITE`'s
`checkin_resets_transaction_and_session_state_test`), not here -- faking a connection that
answers the wire protocol well enough to reset would just be reimplementing `pgc_client`.
""".
process_checkin_dead_connection_test() ->
    ConnectionPid = dead_standin(),
    UserPid = standin(),
    UserMonitor = erlang:monitor(process, UserPid),
    State = test_state([], #{ConnectionPid => #checkout{id = make_ref(), user_pid = UserPid, user_monitor = UserMonitor}}, queue:new()),
    NewState = process_checkin(ConnectionPid, State),
    ?assertEqual([], NewState#state.available),
    ?assertEqual(#{}, NewState#state.used),
    UserPid ! stop.

-doc "Checking in a pid that isn't actually checked out (e.g. a stale/late message) is a no-op.".
process_checkin_unknown_connection_test() ->
    State = test_state([], #{}, queue:new()),
    ?assertEqual(State, process_checkin(self(), State)).

-doc """
A connection dying while checked out drops it from `used`, demonitors its former user, and frees
up its slot -- the same outcome as a normal checkin, minus returning it to `available`.
""".
remove_client_while_used_test() ->
    ConnectionPid = standin(),
    UserPid = standin(),
    UserMonitor = erlang:monitor(process, UserPid),
    State = test_state([], #{ConnectionPid => #checkout{id = make_ref(), user_pid = UserPid, user_monitor = UserMonitor}}, queue:new()),
    NewState = remove_client(ConnectionPid, State),
    ?assertEqual(#{}, NewState#state.used),
    ?assertEqual(State#state.size - 1, NewState#state.size),
    UserPid ! stop.

-doc "A connection dying while idle just drops it from `available` and frees up its slot.".
remove_client_while_available_test() ->
    ConnectionPid = standin(),
    State = test_state([ConnectionPid], #{}, queue:new()),
    NewState = remove_client(ConnectionPid, State),
    ?assertEqual([], NewState#state.available),
    ?assertEqual(State#state.size - 1, NewState#state.size).

-doc """
A checkout_cancel that loses the race against a grant (the connection is already in `used` by the
time it's processed) is treated as an immediate checkin, not silently dropped -- otherwise the
connection would leak into `used` forever, tied to a caller who already gave up.
""".
handle_cast_checkout_cancel_already_granted_test() ->
    ConnectionPid = dead_standin(),
    UserPid = standin(),
    CheckoutId = make_ref(),
    UserMonitor = erlang:monitor(process, UserPid),
    State = test_state([], #{ConnectionPid => #checkout{id = CheckoutId, user_pid = UserPid, user_monitor = UserMonitor}}, queue:new()),
    {noreply, NewState} = handle_cast({checkout_cancel, CheckoutId}, State),
    ?assertEqual([], NewState#state.available),
    ?assertEqual(#{}, NewState#state.used),
    UserPid ! stop.

-doc "A checkout_cancel that beats the grant just removes the matching entry from `waiting`.".
handle_cast_checkout_cancel_still_waiting_test() ->
    UserPid = standin(),
    CheckoutId = make_ref(),
    UserMonitor = erlang:monitor(process, UserPid),
    OtherUserPid = standin(),
    OtherCheckoutId = make_ref(),
    OtherUserMonitor = erlang:monitor(process, OtherUserPid),
    Waiting = queue:from_list([
        {{self(), make_ref()}, #checkout{id = CheckoutId, user_pid = UserPid, user_monitor = UserMonitor}},
        {{self(), make_ref()}, #checkout{id = OtherCheckoutId, user_pid = OtherUserPid, user_monitor = OtherUserMonitor}}
    ]),
    State = (test_state([], #{}, Waiting))#state{size = 0, max_size = 1},
    {noreply, NewState} = handle_cast({checkout_cancel, CheckoutId}, State),
    ?assertEqual(1, queue:len(NewState#state.waiting)),
    ?assertNot(lists:any(fun ({_From, Checkout}) -> Checkout#checkout.id =:= CheckoutId end, queue:to_list(NewState#state.waiting))),
    erlang:demonitor(OtherUserMonitor, [flush]),
    UserPid ! stop,
    OtherUserPid ! stop.

-doc "A connection DOWN is cleaned up and may free a slot for whoever's still waiting.".
handle_info_connection_down_test() ->
    ConnectionPid = standin(),
    State = test_state([ConnectionPid], #{}, queue:new()),
    {noreply, NewState} = handle_info({{'DOWN', connection}, make_ref(), process, ConnectionPid, normal}, State),
    ?assertEqual([], NewState#state.available),
    ?assertEqual(State#state.size - 1, NewState#state.size).

-doc "A checked-out caller dying is treated exactly like it calling checkin/2 itself.".
handle_info_user_down_test() ->
    ConnectionPid = dead_standin(),
    UserPid = standin(),
    CheckoutId = make_ref(),
    UserMonitor = erlang:monitor(process, UserPid),
    State = test_state([], #{ConnectionPid => #checkout{id = CheckoutId, user_pid = UserPid, user_monitor = UserMonitor}}, queue:new()),
    {noreply, NewState} = handle_info({{'DOWN', user}, UserMonitor, process, UserPid, killed}, State),
    ?assertEqual([], NewState#state.available),
    ?assertEqual(#{}, NewState#state.used).
-endif.
