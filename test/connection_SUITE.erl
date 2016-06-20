-module(connection_SUITE).
-export([
    all/0,
    groups/0,
    init_per_suite/1,
    end_per_suite/1,
    init_per_testcase/2,
    end_per_testcase/2
]).

-export([
    iodata/1,
    timeout/1,
    hibernate/1,
    binref_leak/1
]).

-include_lib("common_test/include/ct.hrl").
-include("../include/types.hrl").

all() ->
    [{group, all}].

groups() ->
    [
        {all, [parallel, shuffle], [
            iodata,
            timeout,
            hibernate,
            binref_leak
        ]}
    ].


init_per_suite(Config) ->
    application:load(sasl),
    application:set_env(sasl, errlog_type, error),
    {ok, _} = application:ensure_all_started(sasl),
    {ok, _} = application:ensure_all_started(pgsql),
    Config.

end_per_suite(_Config) ->
    ok = application:stop(pgsql),
    ok.

init_per_testcase(_Case, Config) ->
    {ok, Conn} = pgsql_connection:start_link(#{}, #{user => "postgres", password => ""}),
    [{conn, Conn} | Config].

end_per_testcase(_Case, Config) ->
    Conn = ?config(conn, Config),
    pgsql_connection:stop(Conn),
    ok.

%% Test cases

iodata(Config) ->
    {ok, [_], [{123}]} = execute([["S", $E, [<<"LEC">> | "T"], " ", "123"], []], Config).

timeout(Config) ->
    {error, timeout} = execute("select pg_sleep(0.1)", [], #{timeout => 50}, Config),
    {ok, [_], [{void}]} = execute("select pg_sleep(0.1)", [], #{timeout => 200}, Config).

hibernate(Config) ->
    Conn = ?config(conn, Config),
    {ok, _, _} = execute("select * from pg_user", Config),
    [{memory, M1}] = erlang:process_info(Conn, [memory]),
    timer:sleep(timer:seconds(6)),
    [{current_function, {erlang, hibernate, _}}] = erlang:process_info(Conn, [current_function]),
    [{memory, M2}] = erlang:process_info(Conn, [memory]),
    true = (M2 =< M1).


binref_leak(Config) ->
    Conn = ?config(conn, Config),
    {ok, _, _} = execute("select * from pg_user", Config),
    _ = erlang:garbage_collect(Conn),
    [{binary, []}] = erlang:process_info(Conn, [binary]).

%% Helpers
execute(Query, Config) ->
    execute(Query, [], #{}, Config).

execute(Query, Parameters, Opts, Config) ->
    pgsql_connection:execute(?config(conn, Config), Query, Parameters, Opts).
