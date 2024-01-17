# erlang-pgc

PostgreSQL client for Erlang.


## Examples

```erl
1> {ok, Pool} = pgc_pool:start_link(#{address => {tcp, "localhost", 5432}}, #{
  user => "postgres",
  password => "postgres",
  database => "postgres"
}, #{limit => 1}).
{ok, <0.234.0>}
2> pgc_pool:with_connection(Pool, fun (Connection) ->
  pgc_connection:execute(Connection, {"select 'hello ' || $1 as message", ["world"]})
end).
{ok, #{command => select, columns => [message], rows => 1, notices => []}, [
  #{message => <<"hello world">>}
]}
3> pgc_pool:with_connection(Pool, fun (Connection) ->
  pgc_connection:execute(Connection, {"select row('hello', $1::text) as record", ["world"]})
end).
{ok,#{command => select, columns => [record], rows => 1, notices => []}, [
    #{record => #{1 => <<"hello">>,2 => <<"world">>}}
]}
```

## Setup

You need to add `pgc` as a dependency to your project. If you are using `rebar3`, you can add the following to your `rebar.config`:

```erlang
{deps, [
    {pgc, "0.1.0"}
]}.
```

Also ensure that `pgc` is added as a dependency to your application, by
updating your `.app.src` file:

```erlang
{application, my_app, [

    {applications, [
        kernel,
        stdlib,

        pgc  % <- You need this in your applications list
    ]}
]}.
```

## Usage


## Features

## Data representation