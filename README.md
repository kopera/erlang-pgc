# erlang-pgc

PostgreSQL client for Erlang.


## Examples

```erl
1> {ok, Client} = pgc:start_link(#{address => {tcp, "localhost", 5432}}, #{
  user => "postgres",
  password => "postgres",
  database => "postgres"
}, #{limit => 1}).
{ok, <0.234.0>}
2> pgc:execute(Client, {"select 'hello ' || $1 as message", ["world"]}).
{ok, #{command => select, columns => [message], rows => 1, notices => []}, [
  #{message => <<"hello world">>}
]}
3> pgc:execute(Client, {"select row('hello', $1::text) as record", ["world"]}).
{ok,#{command => select, columns => [record], rows => 1, notices => []}, [
    #{record => #{1 => <<"hello">>, 2 => <<"world">>}}
]}
```

## Setup

You need to add `pgc` as a dependency to your project. If you are using `rebar3`, you can add the following to your `rebar.config`:

```erlang
{deps, [
    {pgc, {git, "git://github.com/kopera/erlang-pgc.git", {branch, "main"}}},
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

TODO

## Features

TODO


## Data representation

TODO