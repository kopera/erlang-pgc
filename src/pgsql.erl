-module(pgsql).
-export([
    activity/2,
    activity/3
]).
-export([
    start_link/4,
    child_spec/5
]).

-behaviour(supervisor).
-export([
    init/1
]).


activity(Client, Fun) ->
    activity(Client, Fun, 5000).

activity(Client, Fun, Timeout) ->
    [Connections] = [Pid || {connections, Pid, _, _} <- supervisor:which_children(Client)],
    case pgsql_connections:checkout(Connections, Timeout) of
        {ok, Connection} ->
            try Fun(Connection) of
                Result -> Result
            after
                pgsql_connections:checkin(Connections, Connection)
            end;
        {error, timeout} = Error ->
            exit(Error)
    end.

-spec start_link(Name, PoolOpts, TransportOpts, DatabaseOpts) -> {ok, pid()} | {error, any()} when
    Name :: pgsql_connections:name(),
    PoolOpts :: pgsql_connections:start_options(),
    TransportOpts :: pgsql_connection:transport_options(),
    DatabaseOpts :: pgsql_connection:database_options().
start_link(Name, PoolOpts, TransportOpts, DatabaseOpts) ->
    supervisor:start_link(?MODULE, {pgsql_sup, [Name, PoolOpts, TransportOpts, DatabaseOpts]}).

-spec child_spec(Id, Name, PoolOpts, TransportOpts, DatabaseOpts) -> supervisor:child_spec() when
    Id :: any(),
    Name :: pgsql_connections:name(),
    PoolOpts :: pgsql_connections:start_options(),
    TransportOpts :: pgsql_connection:transport_options(),
    DatabaseOpts :: pgsql_connection:database_options().
child_spec(Id, Name, PoolOpts, TransportOpts, DatabaseOpts) ->
    #{
        id => Id,
        type => supervisor,
        start => {supervisor, start_link, [?MODULE, {pgsql_sup, [Name, PoolOpts, TransportOpts, DatabaseOpts]}]}
    }.

%% @hidden
init({pgsql_sup, [Name, PoolOpts, TransportOpts, DatabaseOpts]}) ->
    Flags = #{strategy => one_for_all},
    Children = [
        #{
            id => connections_sup,
            type => supervisor,
            start => {supervisor, start_link, [?MODULE, {connections_sup, [TransportOpts, DatabaseOpts]}]}
        },
        #{
            id => connections,
            start => {pgsql_connections, start_link, [self(), Name, PoolOpts]}
        }
    ],
    {ok, {Flags, Children}};
init({connections_sup, [TransportOpts, DatabaseOpts]}) ->
    Flags = #{strategy => simple_one_for_one},
    Children = [
        #{
            id => connection,
            start => {pgsql_connection, start_link, [TransportOpts, DatabaseOpts]},
            restart => temporary
        }
    ],
    {ok, {Flags, Children}}.
