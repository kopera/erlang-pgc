-module(pgc).
-export([
    start_link/3
]).
-export([
    execute/2,
    execute/3
    % transaction/2
]).


%% @doc Start a new PostgreSQL client.
%% 
%% This function is meant to be used as part of a {@link supervisor:child_spec()}.
-spec start_link(TransportOptions, ClientOptions, PoolOptions) -> {ok, pid()} when
    TransportOptions :: transport_options(),
    ClientOptions :: client_options(),
    PoolOptions :: pool_options().
-type transport_options() :: #{
    address := transport_address(),
    tls => disable | prefer | require,
    tls_options => [ssl:tls_client_option()],
    connect_timeout => timeout()
}.
-type transport_address() :: {tcp, inet:ip_address() | inet:hostname(), inet:port_number()}.
-type client_options() :: #{
    user := unicode:chardata(),
    password => unicode:chardata() | fun(() -> unicode:chardata()),
    database := unicode:chardata(),
    parameters => #{
        application_name => unicode:chardata(),
        atom() => unicode:chardata()
    },
    hibernate_after => timeout()
}.
-type pool_options() :: #{
    name => atom(),
    limit => pos_integer()
}.
start_link(TransportOptions, ClientOptions, PoolOptions) ->
    pgc_pool:start_link(TransportOptions, ClientOptions, PoolOptions).



%% @doc Prepare and execute a SQL statement.
%% @equiv execute(Client, Statement, #{})
-spec execute(Client, Statement) -> {ok, Metadata, Rows} | {error, Error} when
    Client :: pid(),
    Statement :: unicode:chardata() | {unicode:unicode_binary(), Parameters} | pgc_statement:template(),
    Parameters :: [term()],
    Metadata :: execute_metadata(),
    Rows :: [term()],
    Error :: pgc_error:t().
execute(Client, Statement) ->
    execute(Client, Statement, #{}).


%% @doc Prepare and execute a SQL statement.
-spec execute(Client, Statement, Options) -> {ok, Metadata, Rows} | {error, Error} when
    Client :: pid(),
    Statement :: unicode:chardata() | {unicode:unicode_binary(), Parameters} | pgc_statement:template(),
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
execute(Client, Statement, _Options) when is_pid(Client); is_atom(Client) ->
    % TODO: split Options into Checkout and Execute Options.
    CheckoutOptions = #{},
    ExecuteOptions = #{},
    {StatementText, StatementParameters} = pgc_statement:new(Statement),
    pgc_pool:with_connection(Client, fun (ConnectionPid) ->
        pgc_connection:execute(ConnectionPid, StatementText, StatementParameters, ExecuteOptions)
    end, CheckoutOptions).