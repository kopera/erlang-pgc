-module(pgsql_demo).
-export([
    execute/2
]).


execute(Statement, Params) ->
    {ok, C} = connect(),
    [{memory, Memory0}] = erlang:process_info(C, [memory]),
    R = pgsql_connection:execute(C, Statement, Params, #{}),
    [{memory, Memory1}] = erlang:process_info(C, [memory]),
    pgsql_connection:stop(C),
    io:format("Result | ~p~n", [R]),
    io:format("Memory | initial: ~b after: ~b~n", [Memory0, Memory1]).

connect() ->
    pgsql_connection:start_link(#{}, #{user => "postgres", password => ""}).
