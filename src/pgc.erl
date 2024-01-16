-module(pgc).
-export([
    connect/2,
    connect/3,
    disconnect/1
]).
-export([
    execute/2,
    execute/3
]).


connect(TransportOptions, ConnectionOptions, PoolOptions) ->
    case pgc_pools_sup:start_pool(TransportOptions, ConnectionOptions, PoolOptions) of
        {ok, _PoolSupervisorPid, PoolManagerPid} ->
            {ok, {pool, PoolManagerPid}}
    end.


-spec connect(TransportOptions, ConnectionOptions) -> {ok, Connection} | {error, Error} when
    TransportOptions :: pgc_transport:options(),
    ConnectionOptions :: pgc_connection:options(),
    Connection :: connection(),
    Error :: pgc_error:t().
-type connection() :: pid().
connect(TransportOptions, ConnectionOptions) ->
    case pgc_connections_sup:start_connection(TransportOptions, ConnectionOptions) of
        {ok, ConnectionPid} ->
            receive
                {pgc_connection, ConnectionPid, connected} ->
                    {ok, ConnectionPid};
                {pgc_connection, ConnectionPid, {error, _} = Error} ->
                    Error
            end;
        {error, _} = Error ->
            Error
    end.


disconnect({pool, PoolManagerPid}) ->
    pgc_pool:stop(PoolManagerPid);
disconnect(Connection) ->
    pgc_connection:stop(Connection).


-spec execute(Connection, Statement) -> {ok, Metadata, Rows} | {error, Error} when
    Connection :: connection(),
    Statement :: unicode:unicode_binary() | {unicode:unicode_binary(), Parameters},
    Parameters :: [term()],
    Metadata :: pgc_connection:execute_metadata(),
    Rows :: [term()],
    Error :: pgc_error:t().
execute(Connection, Statement) ->
    execute(Connection, Statement, #{}).


-spec execute(Connection, Statement, Options) -> {ok, Metadata, Rows} | {error, Error} when
    Connection :: connection(),
    Statement :: unicode:unicode_binary() | {unicode:unicode_binary(), Parameters} | pgc_statement:template(),
    Parameters :: [term()],
    Options :: pgc_connection:execute_options(),
    Metadata :: pgc_connection:execute_metadata(),
    Rows :: [term()],
    Error :: pgc_error:t().
execute({pool, PoolManagerPid}, Statement, Options) ->
    case pgc_pool_manager:checkout(PoolManagerPid, #{}) of
        {ok, Connection} ->
            try execute(Connection, Statement, Options) of
                Result -> Result
            after
                pgc_pool_manager:checkin(PoolManagerPid, Connection)
            end;
        {error, timeout} ->
            exit(pool_timeout);
        {error, Error} ->
            {error, Error}
    end;
execute(Connection, Statement, Options) when is_binary(Statement) ->
    pgc_connection:execute(Connection, Statement, [], Options);
execute(Connection, {Statement, Parameters}, Options) ->
    pgc_connection:execute(Connection, Statement, Parameters, Options);
execute(Connection, StatementTemplate, Options) when is_list(StatementTemplate) ->
    {Statement, Parameters} = pgc_statement:from_template(StatementTemplate),
    pgc_connection:execute(Connection, Statement, Parameters, Options).