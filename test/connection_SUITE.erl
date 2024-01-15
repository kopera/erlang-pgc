-module(connection_SUITE).
-export([
    suite/0,
    all/0,
    groups/0,
    init_per_suite/1,
    end_per_suite/1,
    init_per_testcase/2,
    end_per_testcase/2
]).

-export([
    execute_simple/1,
    hibernate/1,
    binref_leak/1
]).

suite() ->
    [
        {require, address, 'postgresql_server_address'},
        {require, user, 'postgresql_server_user'},
        {require, password, 'postgresql_server_password'},
        {require, database, 'postgresql_server_database'}
    ].

all() ->
    [{group, all}].

groups() ->
    [
        {all, [parallel, shuffle], [
            execute_simple,
            hibernate,
            binref_leak
        ]}
    ].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(pgc),
    Config.

end_per_suite(_Config) ->
    ok = application:stop(pgc),
    ok.

init_per_testcase(_Case, Config) ->
    {ok, Connection} = pgc:connect(#{address => ct:get_config(address)}, #{
        user => ct:get_config(user),
        password => ct:get_config(password),
        database => ct:get_config(database),
        hibernate_after => 500
    }),
    [{connection, Connection} | Config].

end_per_testcase(_Case, Config) ->
    Connection = proplists:get_value(connection, Config),
    pgc:disconnect(Connection),
    ok.

%% Test cases

execute_simple(Config) ->
    Connection = proplists:get_value(connection, Config),
    Statement = "select 123 as test",
    {ok, #{rows := 1}, [#{test := 123}]} = pgc_connection:execute(Connection, Statement, []).



% timeout(Config) ->
%     {error, timeout} = execute("select pg_sleep(0.1)", [], #{timeout => 50}, Config),
%     #{rows := [{void}]} = execute("select pg_sleep(0.1)", [], #{timeout => 200}, Config).


hibernate(Config) ->
    Connection = proplists:get_value(connection, Config),
    [{memory, M0}] = erlang:process_info(Connection, [memory]),
    {ok, #{command := select}, _} = pgc_connection:execute(Connection, "select * from pg_user", []),
    [{memory, M1}] = erlang:process_info(Connection, [memory]),
    timer:sleep(timer:seconds(1)),
    [{current_function, {erlang, hibernate, _}}] = erlang:process_info(Connection, [current_function]),
    [{memory, M2}] = erlang:process_info(Connection, [memory]),
    ct:log([
        "Memory usage before query: ~b bytes~n",
        "Memory usage after query: ~b bytes~n",
        "Memory usage after hibernation: ~b bytes"
    ], [M0, M1, M2]),
    true = (M2 =< M1).


binref_leak(Config) ->
    Connection = proplists:get_value(connection, Config),
    {ok, #{command := select}, Rows} = pgc_connection:execute(Connection, "select * from pg_user", []),
    _ = erlang:garbage_collect(Connection),
    [{binary, []}] = erlang:process_info(Connection, [binary]),
    Rows.