-module(pgc).
-export([
    connect/2,
    disconnect/1,
    execute/2,
    execute/3
]).


-spec connect(TransportOptions, ConnectionOptions) -> {ok, Connection} | {error, Error} when
    TransportOptions :: transport_options(),
    ConnectionOptions :: connection_options(),
    Connection :: connection(),
    Error :: pgc_error:t().
-type transport_options() :: #{
    address := transport_address(),
    tls => disable | prefer | require,
    tls_options => [ssl:tls_client_option()],
    connect_timeout => timeout()
}.
-type transport_address() :: {tcp, inet:ip_address() | inet:hostname(), inet:port_number()}.
-type connection_options() :: #{
    user := unicode:chardata(),
    password => unicode:chardata() | fun(() -> unicode:chardata()),
    database := unicode:chardata(),
    parameters => #{
        application_name => unicode:chardata(),
        atom() => unicode:chardata()
    },

    hibernate_after => timeout()
}.
-type connection() :: pid().
connect(TransportOptions0, ConnectionOptions0) ->
    {
        TransportOptions,
        ClientOptions,
        DatabaseOptions
    } = connect_options(TransportOptions0, ConnectionOptions0),
    case pgc_transport:connect(TransportOptions) of
        {ok, Transport} ->
            case pgc_connections_sup:start_connection(pgc_connections_sup, self(), ClientOptions) of
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
connect_options(TransportOptions, Options) ->
    ClientOptions = maps:with([hibernate_after], Options),
    DefaultParameters = #{
        application_name => atom_to_binary(node()),
        'TimeZone' => <<"Etc/UTC">>
    },
    DatabaseOptions = maps:update_with(parameters, fun (P) ->
        maps:merge(DefaultParameters, P)
    end, DefaultParameters, maps:without([hibernate_after], Options)),
    {TransportOptions, ClientOptions, DatabaseOptions}.


disconnect(Connection) ->
    pgc_connection:close(Connection).


-spec execute(Connection, Statement) -> {ok, Metadata, Rows} | {error, Error} when
    Connection :: connection(),
    Statement :: unicode:unicode_binary() | {unicode:unicode_binary(), Parameters},
    Parameters :: [term()],
    Metadata :: execute_metadata(),
    Rows :: [term()],
    Error :: pgc_error:t().
execute(Connection, Statement) ->
    execute(Connection, Statement, #{}).


-spec execute(Connection, Statement, Options) -> {ok, Metadata, Rows} | {error, Error} when
    Connection :: connection(),
    Statement :: unicode:unicode_binary() | {unicode:unicode_binary(), Parameters} | pgc_statement:template(),
    Parameters :: [term()],
    Options :: execute_options(),
    Metadata :: execute_metadata(),
    Rows :: [term()],
    Error :: pgc_error:t().
-type execute_options() :: #{
    cache => false | {true, atom()},
    row => map | tuple | list | proplist
}.
-type execute_metadata() :: #{
    command := atom(),
    columns := [atom()],
    rows => non_neg_integer(),
    notices := [map()]
}.
execute(Connection, Statement, Options) when is_binary(Statement) ->
    pgc_connection:execute(Connection, Statement, [], Options);
execute(Connection, {Statement, Parameters}, Options) ->
    pgc_connection:execute(Connection, Statement, Parameters, Options);
execute(Connection, StatementTemplate, Options) when is_list(StatementTemplate) ->
    {Statement, Parameters} = pgc_statement:from_template(StatementTemplate),
    pgc_connection:execute(Connection, Statement, Parameters, Options).