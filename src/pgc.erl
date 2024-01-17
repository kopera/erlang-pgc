-module(pgc).
-export([
    start_pool/3,
    stop_pool/1
]).
-export([
    start_connection/2,
    stop_connection/1
]).
-export_type([
    pool_ref/0
]).


-spec start_pool(TransportOptions, ConnectionOptions, PoolOptions) -> {ok, Pool} | {error, Error} when
    TransportOptions :: pgc_transport:options(),
    ConnectionOptions :: pgc_connection:options(),
    PoolOptions :: pgc_pool:options(),
    Pool :: pid(),
    Error :: pgc_error:t().
start_pool(TransportOptions, ConnectionOptions, PoolOptions) ->
    pgc_pools_sup:start_pool(self(), TransportOptions, ConnectionOptions, PoolOptions).


-spec stop_pool(pool_ref()) -> ok.
-type pool_ref() :: pid() | atom().
stop_pool(PoolRef) ->
    pgc_pools_sup:stop_pool(PoolRef).


-spec start_connection(TransportOptions, ConnectionOptions) -> {ok, Connection} | {error, Error} when
    TransportOptions :: pgc_transport:options(),
    ConnectionOptions :: pgc_connection:options(),
    Connection :: pid(),
    Error :: pgc_error:t().
start_connection(TransportOptions, ConnectionOptions) ->
    case pgc_connections_sup:start_connection(self(), TransportOptions, ConnectionOptions) of
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


-spec stop_connection(connection_ref()) -> ok.
-type connection_ref() :: pid().
stop_connection(ConnectionPid) ->
    pgc_connections_sup:stop_connection(ConnectionPid).