-module(pgc).
-export([
    connect/2,
    disconnect/1
]).


-spec connect(TransportOptions, ConnectionOptions) -> {ok, Connection} | {error, Error} when
    TransportOptions :: transport_options(),
    ConnectionOptions :: connection_options(),
    Connection :: pid(),
    Error :: pgc_error:t().
-type transport_options() :: pgc_transport:connect_options().
-type connection_options() :: #{
    user := unicode:chardata(),
    password => unicode:chardata() | fun(() -> unicode:chardata()),
    database := unicode:chardata(),
    parameters => #{atom() => unicode:chardata()},

    hibernate_after => timeout()
}.
connect(TransportOptions0, ConnectionOptions0) ->
    {
        TransportOptions,
        ClientOptions,
        DatabaseOptions
    } = connect_options(TransportOptions0, ConnectionOptions0),
    case pgc_transport:connect(TransportOptions) of
        {ok, Transport} ->
            case pgc_connections_sup:start_connection(self(), ClientOptions) of
                {ok, ConnectionPid} ->
                    ok = pgc_transport:set_owner(Transport, ConnectionPid),
                    case pgc_connection:open(ConnectionPid, Transport, DatabaseOptions) of
                        ok -> {ok, ConnectionPid};
                        {error, _} = Error -> Error
                    end;
                {error, _} = Error ->
                    _ = pgc_transport:close(Transport),
                    Error
            end;
        {error, _} = Error ->
            Error 
    end.


%% @private
connect_options(TransportOptions, ConnectionOptions) ->
    ClientOptions = maps:with([hibernate_after], ConnectionOptions),
    DefaultParameters = #{
        application_name => atom_to_binary(node()),
        'TimeZone' => <<"Etc/UTC">>
    },
    DatabaseOptions = maps:update_with(parameters, fun (P) ->
        maps:merge(DefaultParameters, P)
    end, DefaultParameters, maps:without([hibernate_after], ConnectionOptions)),
    {TransportOptions, ClientOptions, DatabaseOptions}.


disconnect(Connection) ->
    pgc_connection:close(Connection).