-module(pgc).
-export([
    start_link/3
]).
-export([
    execute/2,
    execute/3,
    transaction/2,
    transaction/3,
    rollback/2
]).
-export_type([
    transport_options/0,
    transport_address/0,
    client_options/0,
    execute_options/0,
    execute_metadata/0,
    transaction_options/0,
    transaction/0
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
    ping_interval => timeout()
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
    Parameters :: [dynamic()],
    Metadata :: execute_metadata(),
    Rows :: [#{atom() => dynamic()}],
    Error :: pgc_error:t().
execute(Client, Statement) ->
    execute(Client, Statement, #{}).


%% @doc Prepare and execute a SQL statement.
-spec execute(Client, Statement, Options) -> {ok, Metadata, Rows} | {error, Error} when
    Client :: pid() | atom() | transaction(),
    Statement :: unicode:chardata() | {unicode:unicode_binary(), Parameters} | pgc_statement:template(),
    Parameters :: [term()],
    Options :: execute_options(),
    Metadata :: execute_metadata(),
    Rows :: [dynamic()],
    Error :: pgc_error:t().
-type execute_options() :: #{
    cache => false | {true, atom() | unicode:chardata()},
    row => map | tuple | list | proplist,
    timeout => timeout() | {abs, integer()}
}.
-type execute_metadata() :: #{
    command := atom(),
    columns := [atom()],
    rows => non_neg_integer(),
    notices := [map()]
}.
execute({transaction, TransactionRef}, Statement, Options) when is_reference(TransactionRef) ->
    with_transaction(TransactionRef, fun(ClientPid) ->
        Deadline = pgc_deadline:from_timeout(maps:get(timeout, Options, infinity)),
        Timeout = pgc_deadline:to_abs_timeout(Deadline),
        ExecuteOptions = #{
            cache => maps:get(cache, Options, false),
            row => maps:get(row, Options, map),
            timeout => Timeout
        },
        {StatementText, StatementParameters} = pgc_statement:new(Statement),
        pgc_client:execute(ClientPid, StatementText, StatementParameters, ExecuteOptions)
    end);
execute(Client, Statement, Options) when is_pid(Client); is_atom(Client) ->
    Deadline = pgc_deadline:from_timeout(maps:get(timeout, Options, infinity)),
    Timeout = pgc_deadline:to_abs_timeout(Deadline),
    CheckoutOptions = #{
        timeout => Timeout
    },
    ExecuteOptions = #{
        cache => maps:get(cache, Options, false),
        row => maps:get(row, Options, map),
        timeout => Timeout
    },
    {StatementText, StatementParameters} = pgc_statement:new(Statement),
    pgc_pool:with_client(Client, fun (ClientPid) ->
        pgc_client:execute(ClientPid, StatementText, StatementParameters, ExecuteOptions)
    end, CheckoutOptions).


-spec transaction(Client, Transaction) -> {ok, Result} | {error, dynamic() | pgc_error:t()} when
    Client :: pid(),
    Transaction :: fun((transaction()) -> Result).
transaction(Client, Transaction) ->
    transaction(Client, Transaction, #{}).


-spec transaction(Client, Transaction, Options) -> {ok, Result} | {error, dynamic() | pgc_error:t()} when
    Client :: pid(),
    Transaction :: fun((transaction()) -> Result),
    Options :: transaction_options().
-type transaction_options() :: #{
    isolation => serializable | repeatable_read | read_committed | read_uncommitted | default,
    access => read_write | read_only | default,
    deferrable => boolean() | default
}.
-opaque transaction() :: {transaction, reference()}.
transaction(Client, Transaction, Options) when is_function(Transaction, 1) ->
    case current_transaction() of
        undefined ->
            pgc_pool:with_client(Client, fun (ClientPid) ->
                TransactionRef = make_ref(),
                erlang:put({?MODULE, transaction}, {TransactionRef, ClientPid}),
                try
                    pgc_client:transaction(ClientPid, fun() ->
                        Transaction({transaction, TransactionRef})
                    end, Options)
                after
                    erlang:erase({?MODULE, transaction})
                end
            end, #{});
        _ ->
            erlang:error(in_transaction, [Client, Transaction, Options], [
                {error_info, #{
                    cause => #{
                        general => "cannot start a new transaction inside an existing transaction"
                    }
                }}
            ])
    end.


-spec rollback(Transaction, Reason) -> no_return() when
    Transaction :: transaction(),
    Reason :: term().
rollback({transaction, TransactionRef}, Reason) ->
    with_transaction(TransactionRef, fun(ClientPid) ->
        pgc_client:rollback(ClientPid, Reason)
    end).


%% @private
with_transaction(TransactionRef, Fun) ->
    case current_transaction() of
        {TransactionRef, ClientPid} ->
            Fun(ClientPid);
        _ ->
            erlang:error(not_in_transaction, none, [
                {error_info, #{
                    cause => #{
                        1 => "invalid transaction id",
                        general => "provided transaction ID does not match current transaction"
                    }
                }}
            ])
    end.


%% @private
%% @doc Returns true if the current process is inside a transaction.
-spec current_transaction() -> undefined | {TransactionRef :: reference(), ClientPid :: pid()}.
current_transaction() ->
    get({?MODULE, transaction}).