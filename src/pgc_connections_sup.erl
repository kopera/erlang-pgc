%% @private
-module(pgc_connections_sup).
-export([
    start_connection/1,
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
-spec start_connection(pid()) -> {ok, pid()}.
start_connection(Owner) ->
    case supervisor:start_child(?MODULE, [Owner]) of
        {ok, Connection} when is_pid(Connection) -> {ok, Connection}
    end.


%% @private
-spec stop_connection(pid()) -> ok.
stop_connection(Connection) ->
    ok = supervisor:terminate_child(?MODULE, Connection).


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
        connection()
    ],
    {ok, {Flags, Children}}.


%% @private
-spec connection() -> supervisor:child_spec().
connection() ->
    #{
        id => connection,
        start => {pgc_connection, start_link, []},
        restart => temporary
    }.