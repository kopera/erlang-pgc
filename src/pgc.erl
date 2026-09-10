-module(pgc).
-export([
    start_link/2,
    start_link/3,
    child_spec/3,
    child_spec/4,
    stop/1
]).
-export([
    execute/2,
    execute/3,
    transaction/2,
    transaction/3
]).
-export_type([
    transaction_ref/0,
    transaction_options/0
]).


-spec start_link(pgc_client_options:t(), pgc_pool:options()) -> {ok, pid()}.
start_link(ClientOptions, PoolOptions) ->
    pgc_pool:start_link(ClientOptions, PoolOptions).


-spec start_link(pgc_pool:pool_name(), pgc_client_options:t(), pgc_pool:options()) -> {ok, pid()}.
start_link(Name, ClientOptions, PoolOptions) ->
    pgc_pool:start_link(Name, ClientOptions, PoolOptions).


-spec child_spec(Id, pgc_client_options:t(), pgc_pool:options()) -> supervisor:child_spec() when
    Id :: term().
child_spec(Id, ClientOptions, PoolOptions) ->
    pgc_pool:child_spec(Id, ClientOptions, PoolOptions).


-spec child_spec(Id, pgc_pool:pool_name(), pgc_client_options:t(), pgc_pool:options()) -> supervisor:child_spec() when
    Id :: term().
child_spec(Id, Name, ClientOptions, PoolOptions) ->
    pgc_pool:child_spec(Id, Name, ClientOptions, PoolOptions).


-spec stop(pgc_pool:pool_ref()) -> ok.
stop(PoolRef) ->
    pgc_pool:stop(PoolRef).



-spec execute(TransactionRef | PoolRef, Statement) -> {ok, Metadata, Rows} | {error, Error} when
    TransactionRef :: transaction_ref(),
    PoolRef :: pgc_pool:pool_ref(),
    Statement :: unicode:chardata() | {unicode:chardata(), Parameters} | pgc_statement:template(),
    Parameters :: [dynamic()],
    Metadata :: pgc_client:result_metadata(),
    Rows :: [dynamic()],
    Error :: pgc_client:request_error().
execute(Client, Statement) ->
    execute(Client, Statement, #{}).


-spec execute(TransactionRef | PoolRef, Statement, Options) -> {ok, Metadata, Rows} | {error, Error} when
    TransactionRef :: transaction_ref(),
    PoolRef :: pgc_pool:pool_ref(),
    Statement :: unicode:chardata() | {unicode:chardata(), Parameters} | pgc_statement:template(),
    Parameters :: [dynamic()],
    Options :: pgc_client:execute_options(),
    Metadata :: pgc_client:result_metadata(),
    Rows :: [dynamic()],
    Error :: pgc_client:request_error().
execute(TransactionRef, Statement, Options) when is_reference(TransactionRef) ->
    with_transaction(TransactionRef, fun (ClientRef) ->
        S = pgc_statement:new(Statement),
        pgc_client:execute(ClientRef, pgc_statement:text(S), pgc_statement:parameters(S), Options)
    end);
execute(PoolRef, Statement, Options) ->
    Deadline = pgc_deadline:from_timeout(maps:get(timeout, Options, infinity)),
    Timeout = pgc_deadline:to_abs_timeout(Deadline),
    CheckoutOptions = #{
        timeout => Timeout
    },
    ExecuteOptions = Options,
    S = pgc_statement:new(Statement),
    pgc_pool:with_client(PoolRef, fun (ClientRef) ->
        pgc_client:execute(ClientRef, pgc_statement:text(S), pgc_statement:parameters(S), ExecuteOptions)
    end, CheckoutOptions).



-spec transaction(PoolRef, Transaction) -> Result when
    PoolRef :: pgc_pool:pool_ref(),
    Transaction :: fun((transaction_ref()) -> {commit | rollback, Result}).
transaction(PoolRef, Transaction) ->
    transaction(PoolRef, Transaction, #{}).


-spec transaction(PoolRef, Transaction, Options) -> Result when
    PoolRef :: pgc_pool:pool_ref(),
    Transaction :: fun((transaction_ref()) -> {commit | rollback, Result}),
    Options :: transaction_options().
-type transaction_options() :: #{
    isolation => serializable | repeatable_read | read_committed | read_uncommitted | default,
    access => read_write | read_only | default,
    deferrable => boolean() | default
}.
-type transaction_ref() :: reference().
transaction(PoolRef, Transaction, Options) when is_function(Transaction, 1) ->
    case current_transaction() of
        undefined ->
            pgc_pool:with_client(PoolRef, fun (ClientRef) ->
                TransactionRef = make_ref(),
                erlang:put({?MODULE, transaction}, {TransactionRef, ClientRef}),
                try
                    pgc_client:transaction(ClientRef, fun() ->
                        Transaction(TransactionRef)
                    end, Options)
                after
                    erlang:erase({?MODULE, transaction})
                end
            end, #{});
        _ ->
            erlang:error({pgc, in_transaction}, [PoolRef, Transaction, Options], [
                {error_info, #{
                    cause => #{
                        general => "cannot start a new transaction inside an existing transaction"
                    }
                }}
            ])
    end.


with_transaction(TransactionRef, Fun) ->
    case current_transaction() of
        {TransactionRef, ClientPid} ->
            Fun(ClientPid);
        _ ->
            erlang:error({pgc, not_in_transaction}, none, [
                {error_info, #{
                    cause => #{
                        1 => "invalid transaction id",
                        general => "provided transaction ID does not match current transaction"
                    }
                }}
            ])
    end.


-spec current_transaction() -> undefined | {TransactionRef :: reference(), ClientPid :: pid()}.
current_transaction() ->
    get({?MODULE, transaction}).
