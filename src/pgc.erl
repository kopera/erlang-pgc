-module(pgc).
-export([
    connect/2
]).


-spec connect(TransportOptions, ConnectionOptions) -> {ok, Connection} | {error, Error} when
    TransportOptions :: pgc_transport:connect_options(),
    ConnectionOptions :: pgc_connection:options(),
    Connection :: pid(),
    Error :: pgc_error:t().
connect(TransportOptions, ConnectionOptions) ->
    case pgc_transport:connect(TransportOptions) of
        {ok, Transport} ->
            case pgc_connections_sup:start_connection(self()) of
                {ok, ConnectionPid} ->
                    DefaultParameters = #{
                        application_name => atom_to_binary(node()),
                        'TimeZone' => <<"Etc/UTC">>
                    },
                    ConnectionOptions1 = maps:update_with(parameters, fun (P) ->
                        maps:merge(DefaultParameters, P)
                    end, DefaultParameters, ConnectionOptions),
                    ok = pgc_transport:set_owner(Transport, ConnectionPid),
                    case pgc_connection:startup(ConnectionPid, Transport, ConnectionOptions1) of
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