-module(pgsql_demo).
-export([
    execute/2,
    test_memory/2,
    test_timeout/2
]).


execute(Statement, Params) ->
    {ok, C} = connect(),
    R = pgsql_connection:execute(C, Statement, Params, #{}),
    pgsql_connection:stop(C),
    R.

test_memory(Statement, Params) ->
    {ok, C} = connect(),
    [{memory, Memory0}] = erlang:process_info(C, [memory]),
    R1 = pgsql_connection:execute(C, Statement, Params, #{}),
    [{memory, Memory1}] = erlang:process_info(C, [memory]),
    timer:sleep(timer:seconds(5)),
    [{memory, Memory2}] = erlang:process_info(C, [memory]),
    io:format("Result | ~p~n", [R1]),
    io:format("Memory | initial: ~b m1: ~b m2: ~b~n", [Memory0, Memory1, Memory2]),
    pgsql_connection:stop(C).

test_timeout(Sleep, Timeout) ->
    {ok, C} = connect(),
    R1 = pgsql_connection:execute(C, "select pg_sleep($1)", [Sleep], #{timeout => timer:seconds(Timeout)}),
    pgsql_connection:stop(C),
    R1.


connect() ->
    pgsql_connection:start_link(#{}, #{user => "postgres", password => ""}).
