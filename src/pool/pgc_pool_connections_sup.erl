%% @private
-module(pgc_pool_connections_sup).
-export([
    start_connection/1
]).

-export([
    start_link/2
]).

-behaviour(supervisor).
-export([
    init/1
]).


%% @private
-spec start_connection(Supervisor) -> {ok, pid()} when
    Supervisor :: pid().
start_connection(Supervisor) ->
    case supervisor:start_child(Supervisor, [self()]) of
        {ok, Connection} when is_pid(Connection) -> {ok, Connection}
    end.


%% @private
-spec start_link(TransportOptions, ConnectionOptions) -> {ok, pid()} when
    TransportOptions :: pgc_transport:options(),
    ConnectionOptions :: pgc_connection:options().
start_link(TransportOptions, ConnectionOptions) ->
    {ok, _} = supervisor:start_link(?MODULE, {TransportOptions, ConnectionOptions}).


%%====================================================================
%% Supervisor callbacks
%%====================================================================


%% @hidden
-spec init({TransportOptions, ConnectionOptions}) -> {ok, {Flags, [ChildSpec]}} when
    TransportOptions :: pgc_transport:options(),
    ConnectionOptions :: pgc_connection:options(),
    Flags :: supervisor:sup_flags(),
    ChildSpec :: supervisor:child_spec().
init({TransportOptions, ConnectionOptions}) ->
    Flags = #{strategy => simple_one_for_one},
    Children = [
        #{
            id => connection,
            start => {pgc_connection, start_link, [TransportOptions, ConnectionOptions]},
            restart => temporary
        }
    ],
    {ok, {Flags, Children}}.