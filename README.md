# erlang-pgc

PostgreSQL client for Erlang.


## Examples

```erl
1> {ok, Connection} = pgc:connect(#{address => {tcp, "localhost", 5432}}, #{
    user => "postgres",
    password => "postgres",
    database => "postgres"
}).
{ok, <0.234.0>}
2> pgc:execute(Connection, {"select 'hello ' || $1 as message", ["world"]}).
{ok, #{command => select, columns => [message], rows => 1, notices => []}, [
    #{message => <<"hello world">>}
]}
3> pgc:execute(Connection, {"select row('hello', $1::text) as record", ["world"]}).
{ok,#{command => select, columns => [record], rows => 1, notices => []}, [
    #{record => #{1 => <<"hello">>,2 => <<"world">>}}
]}
```

## Features

## Data representation