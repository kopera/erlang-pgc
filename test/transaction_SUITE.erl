-module(transaction_SUITE).
-export([
    all/0,
    groups/0,
    init_per_suite/1,
    end_per_suite/1,
    init_per_testcase/2,
    end_per_testcase/2
]).

-export([
    transaction_commit/1,
    transaction_rollback/1,
    transaction_nesting/1
]).

-include_lib("common_test/include/ct.hrl").
-include("../include/types.hrl").

all() ->
    [{group, all}].

groups() ->
    [
        {all, [parallel, shuffle], [
            transaction_commit,
            transaction_rollback,
            transaction_nesting
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

transaction_commit(Config) ->
    Conn = ?config(conn, Config),
    {ok, 1} = pgsql_connection:transaction(Conn, fun () ->
        {ok, [_], [{Value}]} = pgsql_connection:execute(Conn, "SELECT 1", [], #{}),
        {ok, Value}
    end, #{}).

transaction_rollback(Config) ->
    Conn = ?config(conn, Config),
    {ok, 1} = pgsql_connection:transaction(Conn, fun () ->
        {ok, [_], [{Value}]} = pgsql_connection:execute(Conn, "SELECT 1", [], #{}),
        throw({ok, Value})
    end, #{}).

transaction_nesting(Config) ->
    Conn = ?config(conn, Config),
    {ok, [], []} = pgsql_connection:execute(Conn, "CREATE TEMPORARY TABLE fruits (
        id integer PRIMARY KEY,
        name varchar NOT NULL
    )", [], #{}),

    ok = pgsql_connection:transaction(Conn, fun () ->
        {ok, _, _} = pgsql_connection:execute(Conn, "INSERT INTO fruits (id, name) VALUES (1, 'melon')", [], #{}),
        {ok, [_, _], [{1, <<"melon">>}]} = pgsql_connection:execute(Conn, "SELECT id, name FROM fruits", [], #{}),
        oops = pgsql_connection:transaction(Conn, fun () ->
            {ok, _, _} = pgsql_connection:execute(Conn, "INSERT INTO fruits (id, name) VALUES (2, 'orange'), (3, 'date'), (4, 'olive')", [], #{}),
            {ok, [_, _], [{1, <<"melon">>}, {2, <<"orange">>}, {3, <<"date">>}, {4, <<"olive">>}]}
                = pgsql_connection:execute(Conn, "SELECT id, name FROM fruits ORDER BY id ASC", [], #{}),
            throw(oops) %% Rollback savepoint
        end, #{}),
        {ok, [_, _], [{1, <<"melon">>}]} = pgsql_connection:execute(Conn, "SELECT id, name FROM fruits", [], #{}),
        ok
    end, #{}),
    {ok, [_, _], [{1, <<"melon">>}]} = pgsql_connection:execute(Conn, "SELECT id, name FROM fruits", [], #{}).
