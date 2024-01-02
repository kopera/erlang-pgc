%% @private
-module(pgc_sup).
-export([
    start_link/0
]).

-behaviour(supervisor).
-export([
    init/1
]).


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
    Flags = #{strategy => one_for_one},
    {ok, {Flags, [
        connections_sup()
    ]}}.


%% @private
-spec connections_sup() -> supervisor:child_spec().
connections_sup() ->
    #{
        id => connections_sup,
        start => {pgc_connections_sup, start_link, []},
        shutdown => infinity,
        type => supervisor
    }.