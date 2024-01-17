%% @doc PostgreSQL client connection pool.
-module(pgc_pool).
-export([
    info/1,
    with_connection/2,
    with_connection/3
]).
-export_type([
    options/0
]).

-export([
    start_link/3,
    start_link/4,
    stop/1
]).
-export([
    format_error/2
]).

-behaviour(supervisor).
-export([
    init/1
]).


%% @doc Returns a map containing status information about the pool.
%% 
%% The map contains the following keys:
%% 
%% <dl>
%%  <dt>`starting'</dt>
%%  <dd>the number of connections being started;</dd>
%%  <dt>`available'</dt>
%%  <dd>the number of connections available for use;</dd>
%%  <dt>`used'</dt>
%%  <dd>the number of connections currently in use;</dd>
%%  <dt>`size'</dt>
%%  <dd>the total number of connections, including the ones being started;</dd>
%%  <dt>`limit'</dt>
%%  <dd>the maximum allowed size for the pool</dd>
%% </dl>
-spec info(PoolRef) -> Info when
    PoolRef :: atom() | pid(),
    Info :: #{
        starting := non_neg_integer(),
        available := non_neg_integer(),
        used := non_neg_integer(),
        size := non_neg_integer(),
        limit := pos_integer()
    }.
info(PoolRef) ->
    PoolManagerRef = pool_manager_ref(PoolRef),
    pgc_pool_manager:info(PoolManagerRef).


%% @equiv with_connection(Pool, Action, #{})
-spec with_connection(PoolRef, Action) -> Result when
    PoolRef :: pid() | atom(),
    Action :: fun((Connection :: pid()) -> Result).
with_connection(Pool, Action) ->
    with_connection(Pool, Action, #{}).


%% @doc Checkout a connection from the pool, then call the `Action' function
%% with the connection as only parameter. Returns the connection to the pool
%% upon return or process exit.
%% 
%% The connection can be used with the {@link pgc_connection} module.
%% 
%% The following options can be used to control the checkout behaviour:
%% <dl>
%%  <dt>`timeout'</dt>
%%  <dd>connection checkout timeout (default: `5000ms')</dd>
%% </dl>
%% 
-spec with_connection(PoolRef, Action, Options) -> Result when
    PoolRef :: pid() | atom(),
    Action :: fun((Connection :: pid()) -> Result),
    Options :: checkout_options().
with_connection(Pool, Action, Options) ->
    case checkout(Pool, Options) of
        {ok, ConnectionPid} ->
            try Action(ConnectionPid) of
                Result -> Result
            after
                checkin(Pool, ConnectionPid)
            end;
        {error, timeout} ->
            error(pool_timeout, [Pool, Action, Options], {error_info,  #{
                cause => #{
                    general => "Connection checkout timed out"
                }
            }});
        {error, #{message := Message}} ->
            error(pool_error, [Pool, Action, Options], {error_info,  #{
                cause => #{
                    general => Message
                }
            }})
    end.


%% @private
-spec checkout(PoolRef, Options) -> {ok, Connection} | {error, timeout} when
    PoolRef :: atom() | pid(),
    Options :: checkout_options(),
    Connection :: pid().
-type checkout_options() :: #{
    timeout => timeout() | {abs, timeout()}
}.
checkout(PoolRef, Options) ->
    PoolManagerRef = pool_manager_ref(PoolRef),
    pgc_pool_manager:checkout(PoolManagerRef, Options).


%% @private
-spec checkin(PoolRef, Connection) -> ok when
    PoolRef :: atom() | pid(),
    Connection :: pid().
checkin(PoolRef, ConnectionPid) ->
    PoolManagerRef = pool_manager_ref(PoolRef),
    pgc_pool_manager:checkin(PoolManagerRef, ConnectionPid).


%% @doc Start a new PostgreSQL client pool.
%% 
%% This function is meant to be used as part of a {@link supervisor:child_spec()}.
-spec start_link(TransportOptions, ConnectionOptions, PoolOptions) -> {ok, pid()} when
    TransportOptions :: pgc_transport:options(),
    ConnectionOptions :: pgc_connection:options(),
    PoolOptions :: options().
-type options() :: #{
    name => atom(),
    limit => pos_integer()
}.
start_link(TransportOptions, ConnectionOptions, Options) ->
    {ok, _} = supervisor:start_link(?MODULE, {undefined, TransportOptions, ConnectionOptions, Options}).


-spec start_link(OwnerPid, TransportOptions, ConnectionOptions, PoolOptions) -> {ok, pid()} when
    OwnerPid :: pid(),
    TransportOptions :: pgc_transport:options(),
    ConnectionOptions :: pgc_connection:options(),
    PoolOptions :: options().
start_link(OwnerPid, TransportOptions, ConnectionOptions, Options) ->
    {ok, _} = supervisor:start_link(?MODULE, {OwnerPid, TransportOptions, ConnectionOptions, Options}).


%% @private
-spec stop(PoolRef) -> ok when
    PoolRef :: atom() | pid().
stop(PoolRef) ->
    PoolManagerRef = pool_manager_ref(PoolRef),
    pgc_pool_manager:stop(PoolManagerRef).


-spec pool_manager_ref(atom() | pid()) -> atom() | pid().
pool_manager_ref(PoolName) when is_atom(PoolName) ->
    %% The pool uses a registered name, which in this case refers directly to
    %% the pool manager.
    PoolName;
pool_manager_ref(PoolSupervisorPid) when is_pid(PoolSupervisorPid) ->
    case [Pid || {manager, Pid, _, _} <- supervisor:which_children(PoolSupervisorPid)] of
        [PoolManagerPid] when is_pid(PoolManagerPid) ->
            PoolManagerPid;
        [restarting] ->
            erlang:exit(noproc)
    end.


%% @private
format_error(_Reason, [{_M, _F, _As, Info}|_]) ->
    ErrorInfo = proplists:get_value(error_info, Info, #{}),
    maps:get(cause, ErrorInfo).


%%====================================================================
%% Supervisor callbacks
%%====================================================================

%% @private
-spec init({OwnerPid, TransportOptions, ConnectionOptions, Options}) -> {ok, {Flags, [ChildSpec]}} when
    OwnerPid :: pid() | undefined,
    TransportOptions :: pgc_transport:options(),
    ConnectionOptions :: pgc_connection:options(),
    Options :: options(),
    Flags :: supervisor:sup_flags(),
    ChildSpec :: supervisor:child_spec().
init({OwnerPid, TransportOptions, ConnectionOptions, Options}) ->
    SupervisorPid = self(),
    Flags = #{strategy => one_for_all, auto_shutdown => any_significant},
    {ok, {Flags, [
        #{
            id => connections_sup,
            start => {pgc_pool_connections_sup, start_link, [TransportOptions, ConnectionOptions]},
            type => supervisor
        },
        #{
            id => manager,
            start => {pgc_pool_manager, start_link, [OwnerPid, SupervisorPid, Options]},
            restart => transient,
            significant => true
        }
    ]}}.