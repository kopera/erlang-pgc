%% @private
-module(pgc_pools_sup).
-export([
    start_pool/4,
    stop_pool/1
]).

-export([
    start_link/0
]).

-behaviour(supervisor).
-export([
    init/1
]).


-spec start_pool(OwnerPid, TransportOptions, ConnectionOptions, PoolOptions) -> {ok, pid()} when
    OwnerPid :: pid(),
    TransportOptions :: pgc_transport:options(),
    ConnectionOptions :: pgc_connection:options(),
    PoolOptions :: pgc_pool:options().
start_pool(OwnerPid, TransportOptions, ConnectionOptions, PoolOptions) ->
    case supervisor:start_child(?MODULE, [OwnerPid, TransportOptions, ConnectionOptions, PoolOptions]) of
        {ok, PoolPid} ->
            {ok, PoolPid}
    end.


-spec stop_pool(PoolRef) -> ok when
    PoolRef :: pid() | atom().
stop_pool(PoolRef) ->
    pgc_pool:stop(PoolRef).


%% @private
-spec start_link() -> {ok, pid()}.
start_link() ->
    {ok, _} = supervisor:start_link({local, ?MODULE}, ?MODULE, []).


%%====================================================================
%% Supervisor callbacks
%%====================================================================


%% @private
-spec init([]) -> {ok, {Flags, [ChildSpec]}} when
    Flags :: supervisor:sup_flags(),
    ChildSpec :: supervisor:child_spec().
init([]) ->
    Flags = #{strategy => simple_one_for_one},
    Children = [
        #{
            id => pool,
            start => {pgc_pool, start_link, []},
            restart => transient,
            type => supervisor
        }
    ],
    {ok, {Flags, Children}}.