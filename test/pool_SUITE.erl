-module(pool_SUITE).
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
    execute_simple/1
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
            execute_simple
        ]}
    ].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(pgc),
    Config.

end_per_suite(_Config) ->
    ok = application:stop(pgc),
    ok.

init_per_testcase(_Case, Config) ->
    {ok, Pool} = pgc:connect(#{address => ct:get_config(address)}, #{
        user => ct:get_config(user),
        password => ct:get_config(password),
        database => ct:get_config(database),
        hibernate_after => 500
    }, #{limit => 1}),
    [{connection, Pool} | Config].

end_per_testcase(_Case, Config) ->
    Connection = proplists:get_value(connection, Config),
    pgc:disconnect(Connection),
    ok.

%% Test cases

execute_simple(Config) ->
    Connection = proplists:get_value(connection, Config),
    Statement = ["select 123 as ", <<"test">>],
    {ok, #{rows := 1}, [#{test := 123}]} = pgc:execute(Connection, Statement).