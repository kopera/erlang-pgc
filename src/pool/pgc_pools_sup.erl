%% @private
-module(pgc_pools_sup).
-export([
    start_pool/3
]).

-export([
    start_link/0
]).

-behaviour(supervisor).
-export([
    init/1
]).


%% @private
-spec start_pool(TransportOptions, ConnectionOptions, PoolOptions) -> {ok, pid(), pid()} when
    TransportOptions :: pgc_transport:options(),
    ConnectionOptions :: pgc_connection:options(),
    PoolOptions :: pgc_pool:options().
start_pool(TransportOptions, ConnectionOptions, PoolOptions) ->
    case supervisor:start_child(?MODULE, [TransportOptions, ConnectionOptions, PoolOptions]) of
        {ok, PoolPid, #{manager := ManagerPid}} when is_pid(ManagerPid) ->
            {ok, PoolPid, ManagerPid}
    end.


%% @private
-spec start_link() -> {ok, pid()}.
start_link() ->
    {ok, _} = supervisor:start_link({local, ?MODULE}, ?MODULE, []).


%%====================================================================
%% Supervisor callbacks
%%====================================================================


%% @hidden
-spec init([]) -> {ok, {Flags, [ChildSpec]}} when
    Flags :: supervisor:sup_flags(),
    ChildSpec :: supervisor:child_spec().
init([]) ->
    Flags = #{strategy => simple_one_for_one},
    Children = [
        #{
            id => connection,
            start => {pgc_pool, start_link, []},
            restart => temporary,
            type => supervisor
        }
    ],
    {ok, {Flags, Children}}.