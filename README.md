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
Of course :)

### Transactions
We need to add support for transactions, including nested transactions (using savepoints).
