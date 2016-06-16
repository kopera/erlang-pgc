-module(pgsql_demo).
-export([
    main/2
]).

main(Statement, Params) ->
    {ok, C} = pgsql_connection:start_link(#{}, #{user => "postgres", password => ""}),
    Result = pgsql_connection:execute(C, Statement, Params, #{}),
    pgsql_connection:stop(C),
    Result.
