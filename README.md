# erlang-pgsql

PostgreSQL driver for Erlang. This driver is heavily inspired by [postgrex](https://github.com/elixir-ecto/postgrex),
the PostgreSQL driver for Elixir.

## Example (non-pooled)
```
1> {ok, C} = pgsql_connection:start_link(#{}, #{user => "postgres", password => ""}).
{ok,<0.158.0>}
2> pgsql_connection:execute(C, "select $1::int + $2::int as sum", [2, 4], #{}).
{ok,{[<<"sum">>],[[6]]}}
3> pgsql_connection:stop(C).
ok
```

## TODO

### Pooling
This is handled by gen_db_client, just a matter of wiring it in, gen_db_client pooling should be improved by making use
of sbroker.

### Result Streaming
Currently the entire result set is buffered in the pgsql_connection process, this is quite sub-optimal, the result set
should be either pushed to the client process, or we should give the client process a direct access to the connection
and let it lazily read the result set from the socket.

### Timeouts
We need timeouts, queries that hang should get automatically cancelled by sending a cancel message.

### Type server?
We use a data structure for the codec logic, might be better to use an ETS table?

### Transactions
We need to add support for transactions, including nested transactions (using savepoints).
