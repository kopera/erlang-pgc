-module(pgc_pool_client_sup).
-moduledoc false.

-export([
    start_link/1,
    start_connection/1
]).

-behaviour(supervisor).
-export([
    init/1
]).


-spec start_link(ClientOptions) -> {ok, pid()} when
    ClientOptions :: pgc_client:start_options().
start_link(ClientOptions) ->
    {ok, _} = supervisor:start_link(?MODULE, ClientOptions).


-spec start_connection(Supervisor) -> {ok, pid()} when
    Supervisor :: pid().
start_connection(Supervisor) ->
    case supervisor:start_child(Supervisor, []) of
        {ok, Connection} when is_pid(Connection) -> {ok, Connection}
    end.



% ------------------------------------------------------------------------------
% Supervisor callbacks
% ------------------------------------------------------------------------------

-doc false.
-spec init(ClientOptions) -> {ok, {Flags, [ChildSpec]}} when
    ClientOptions :: pgc_client:start_options(),
    Flags :: supervisor:sup_flags(),
    ChildSpec :: supervisor:child_spec().
init(ClientOptions) ->
    Flags = #{strategy => simple_one_for_one},
    Children = [
        #{
            id => client,
            start => {pgc_client, start_link, [ClientOptions]},
            restart => temporary
        }
    ],
    {ok, {Flags, Children}}.
