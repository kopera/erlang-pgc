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
    transaction_rollback/1
]).

-include_lib("common_test/include/ct.hrl").
-include("../include/types.hrl").

all() ->
    [{group, all}].

groups() ->
    [
        {all, [parallel, shuffle], [
            transaction_commit,
            transaction_rollback
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
    {ok, 1} = pgsql_connection:transaction(?config(conn, Config), fun (Conn) ->
        {ok, [_], [{Value}]} = pgsql_connection:execute(Conn, "select 1", [], #{}),
        {ok, Value}
    end, #{}).

transaction_rollback(Config) ->
    {ok, 1} = pgsql_connection:transaction(?config(conn, Config), fun (Conn) ->
        {ok, [_], [{Value}]} = pgsql_connection:execute(Conn, "select 1", [], #{}),
        throw({ok, Value})
    end, #{}).
