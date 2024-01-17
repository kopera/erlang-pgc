%% @private
-module(pgc_connections_sup).
-export([
    start_connection/3,
    stop_connection/1
]).

-export([
    start_link/0
]).

-behaviour(supervisor).
-export([
    init/1
]).


%% @private
-spec start_connection(OwnerPid, TransportOptions, ConnectionOptions) -> {ok, pid()} when
    OwnerPid :: pid(),
    TransportOptions :: pgc_transport:options(),
    ConnectionOptions :: pgc_connection:options().
start_connection(OwnerPid, TransportOptions, ConnectionOptions) ->
    case supervisor:start_child(?MODULE, [OwnerPid, TransportOptions, ConnectionOptions]) of
        {ok, Connection} when is_pid(Connection) -> {ok, Connection}
    end.


-spec stop_connection(ConnectionRef) -> ok when
    ConnectionRef :: pid() | atom().
stop_connection(ConnectionRef) ->
    pgc_connection:stop(ConnectionRef).


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
            id => connection,
            start => {pgc_connection, start_link, []},
            restart => temporary
        }
    ],
    {ok, {Flags, Children}}.