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
Replace the basic pooling with sbroker

### Keep alive
We need to send periodic sync commands on Idle to keep the connection alive.

### Cache prepared queries
The current API is not ideal in a pooled environment, it might indeed be better to automatically prepare and
cache the prepared statements, with a least recently used cache eviction logic.

### Add support for named parameters
We currently handle the $index syntax for parameters, it might be beneficial to support a ${name} syntax, where the
parameters are passed in as a map of #{atom() => term()} instead of a list.
