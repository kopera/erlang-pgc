-module(pgc_pool).
-export_type([
    options/0
]).

-export([
    start_link/3,
    stop/1
]).

-behaviour(supervisor).
-export([
    init/1
]).


%% @private
-spec start_link(TransportOptions, ConnectionOptions, Options) -> {ok, pid(), Info} when
    TransportOptions :: pgc_transport:options(),
    ConnectionOptions :: pgc_connection:options(),
    Options :: options(),
    Info :: #{manager := pid()}.
-type options() :: #{
    name => atom(),
    limit => pos_integer()
}.
start_link(TransportOptions, ConnectionOptions, Options) ->
    {ok, SupervisorPid} = supervisor:start_link(?MODULE, {TransportOptions, ConnectionOptions, Options}),
    [ManagerPid] = [Pid || {manager, Pid, _, _} <- supervisor:which_children(SupervisorPid)],
    {ok, SupervisorPid, #{
        manager => ManagerPid
    }}.


stop(PoolManagerPid) ->
    pgc_pool_manager:stop(PoolManagerPid).


%%====================================================================
%% Supervisor callbacks
%%====================================================================


%% @hidden
-spec init({TransportOptions, ConnectionOptions, Options}) -> {ok, {Flags, [ChildSpec]}} when
    TransportOptions :: pgc_transport:options(),
    ConnectionOptions :: pgc_connection:options(),
    Options :: options(),
    Flags :: supervisor:sup_flags(),
    ChildSpec :: supervisor:child_spec().
init({TransportOptions, ConnectionOptions, Options}) ->
    Flags = #{strategy => one_for_all, auto_shutdown => any_significant},
    {ok, {Flags, [
        #{
            id => connections_sup,
            start => {pgc_pool_connections_sup, start_link, [TransportOptions, ConnectionOptions]},
            type => supervisor
        },
        #{
            id => manager,
            start => {pgc_pool_manager, start_link, [self(), Options]},
            restart => temporary,
            significant => true
        }
    ]}}.