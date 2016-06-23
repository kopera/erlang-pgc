-module(pgsql).
-export([
    activity/2,
    activity/3
]).
-export([
    start_link/5,
    child_spec/6
]).

-behaviour(supervisor).
-export([
    init/1
]).


activity(Name, Fun) ->
    activity(Name, Fun, 5000).

activity(Name, Fun, Timeout) ->
    case pgsql_connections:checkout(Name, Timeout) of
        {ok, Connection} ->
            try Fun(Connection) of
                Result -> Result
            after
                pgsql_connections:checkin(Name, Connection)
            end;
        {error, timeout} = Error ->
            exit(Error)
    end.

-spec start_link(Name, PoolOpts, TransportOpts, DatabaseOpts, ConnectionOpts) -> {ok, pid()} | {error, any()} when
    Name :: pgsql_connections:name(),
    PoolOpts :: pgsql_connections:start_options(),
    TransportOpts :: pgsql_connection:transport_options(),
    DatabaseOpts :: pgsql_connection:database_options(),
    ConnectionOpts :: pgsql_connection:connection_options().
start_link(Name, PoolOpts, TransportOpts, DatabaseOpts, ConnectionOpts) ->
    supervisor:start_link(?MODULE, {pgsql_sup, [Name, PoolOpts, TransportOpts, DatabaseOpts, ConnectionOpts]}).

-spec child_spec(Id, Name, PoolOpts, TransportOpts, DatabaseOpts, ConnectionOpts) -> supervisor:child_spec() when
    Id :: any(),
    Name :: pgsql_connections:name(),
    PoolOpts :: pgsql_connections:start_options(),
    TransportOpts :: pgsql_connection:transport_options(),
    DatabaseOpts :: pgsql_connection:database_options(),
    ConnectionOpts :: pgsql_connection:connection_options().
child_spec(Id, Name, PoolOpts, TransportOpts, DatabaseOpts, ConnectionOpts) ->
    #{
        id => Id,
        type => supervisor,
        start => {supervisor, start_link, [?MODULE, {pgsql_sup, [Name, PoolOpts, TransportOpts, DatabaseOpts, ConnectionOpts]}]}
    }.

%% @hidden
init({pgsql_sup, [Name, PoolOpts, TransportOpts, DatabaseOpts, ConnectionOpts]}) ->
    Flags = #{strategy => one_for_all},
    Children = [
        #{
            id => connections_sup,
            type => supervisor,
            start => {supervisor, start_link, [?MODULE, {connections_sup, [TransportOpts, DatabaseOpts, ConnectionOpts]}]}
        },
        #{
            id => connections,
            start => {pgsql_connections, start_link, [self(), Name, PoolOpts]}
        }
    ],
    {ok, {Flags, Children}};
init({connections_sup, [TransportOpts, DatabaseOpts, ConnectionOpts]}) ->
    Flags = #{strategy => simple_one_for_one},
    Children = [
        #{
            id => connection,
            start => {pgsql_connection, start_link, [TransportOpts, DatabaseOpts, ConnectionOpts]},
            restart => temporary
        }
    ],
    {ok, {Flags, Children}}.
