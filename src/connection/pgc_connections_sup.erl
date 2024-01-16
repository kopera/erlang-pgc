%% @private
-module(pgc_connections_sup).
-export([
    start_connection/3
]).

-export([
    start_link/0,
    start_link/1
]).

-behaviour(supervisor).
-export([
    init/1
]).


%% @private
-spec start_connection(Supervisor, Owner, Options) -> {ok, pid()} when
    Supervisor :: atom() | pid(),
    Owner :: pid(),
    Options :: #{
        hibernate_after => timeout()
    }.
start_connection(Supervisor, Owner, Options) ->
    case supervisor:start_child(Supervisor, [Owner, Options]) of
        {ok, Connection} when is_pid(Connection) -> {ok, Connection}
    end.


%% @private
-spec start_link() -> {ok, pid()}.
start_link() ->
    {ok, _} = supervisor:start_link(?MODULE, []).

%% @private
-spec start_link(atom()) -> {ok, pid()}.
start_link(Name) ->
    {ok, _} = supervisor:start_link({local, Name}, ?MODULE, []).


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