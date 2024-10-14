-module(pgc_connection_SUITE).

-behaviour(ct_suite).
-export([
    suite/0,
    init_per_suite/1,
    end_per_suite/1,
    init_per_testcase/2,
    end_per_testcase/2,
    all/0,
    groups/0
]).

-export([
    select_basic_test/1,
    select_uuid_test/1,
    select_array_test/1,
    select_time_test/1,
    select_record_test/1,
    select_oid_test/1,
    select_void_test/1,
    select_xid8_test/1,
    select_empty_test/1,
    select_ltree_test/1,
    select_interval_test/1
]).
-export([
    transaction_commit_test/1,
    transaction_rollback_test/1,
    transaction_error_test/1
]).
-export([
    execute_timeout_test/1,
    hibernate_test/1,
    binref_leak_test/1
]).

-define(ASSERT, true).
-include_lib("stdlib/include/assert.hrl").


%% @doc https://www.erlang.org/doc/man/ct_suite#Module:suite-0
suite() ->
    [
        {require, address, 'postgresql_server_address'},
        {require, user, 'postgresql_server_user'},
        {require, password, 'postgresql_server_password'},
        {require, database, 'postgresql_server_database'}
    ].

%% @doc https://www.erlang.org/doc/man/ct_suite#Module:init_per_suite-1
init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(pgc),
    Config.

%% @doc https://www.erlang.org/doc/man/ct_suite#Module:end_per_suite-1
end_per_suite(_Config) ->
    ok = application:stop(pgc),
    ok.

%% @doc https://www.erlang.org/doc/man/ct_suite#Module:init_per_testcase-2
init_per_testcase(_Case, Config) ->
    TransportOptions = #{
        address => ct:get_config(address)
    },
    ConnectionOptions = #{
        user => ct:get_config(user),
        password => ct:get_config(password),
        database => ct:get_config(database),
        ping_interval => 500
    },
    {ok, Connection} = pgc_connection:start_link(TransportOptions, ConnectionOptions, self()),
    {ok, _,[]} = pgc_client:execute(Connection, "CREATE EXTENSION IF NOT EXISTS ltree", []),
    [{connection, Connection} | Config].

%% @doc https://www.erlang.org/doc/man/ct_suite#Module:end_per_testcase-2
end_per_testcase(_Case, Config) ->
    Connection = proplists:get_value(connection, Config),
    pgc_connection:stop(Connection),
    ok.

%% @doc https://www.erlang.org/doc/man/ct_suite#Module:all-0
all() ->
    [
        {group, select},
        {group, transaction},
        execute_timeout_test,
        hibernate_test,
        binref_leak_test
    ].

%% @doc https://www.erlang.org/doc/man/ct_suite#Module:groups-0
groups() ->
    [
        {select, [shuffle], [
            select_basic_test,
            select_uuid_test,
            select_array_test,
            select_time_test,
            select_record_test,
            select_oid_test,
            select_void_test,
            select_xid8_test,
            select_empty_test,
            select_ltree_test,
            select_interval_test
        ]},
        {transaction, [], [
            transaction_commit_test,
            transaction_rollback_test,
            transaction_error_test
        ]}
    ].


% ------------------------------------------------------------------------------
% Test cases
% ------------------------------------------------------------------------------

select_basic_test(Config) ->
    select_test(Config, [
        {"null",                            {null}},
        {"true, false",                     {true, false}},
        {"'e'::char",                       {<<"e">>}},
        {"'å'::char",                       {<<"å"/utf8>>}},
        {"'åja'",                           {<<"åja"/utf8>>}},
        {"42",                              {42}},
        {"42::float4",                      {42.0}},
        {"42::float8",                      {42.0}},
        {"'NaN'::float4",                   {'NaN'}},
        {"'NaN'::float8",                   {'NaN'}},
        {"'inf'::float4",                   {'infinity'}},
        {"'inf'::float8",                   {'infinity'}},
        {"'-inf'::float4",                  {'-infinity'}},
        {"'-inf'::float8",                  {'-infinity'}},
        {"'\\001\\002\\003'::bytea",        {<<1, 2, 3>>}},

        {{"$1::integer", [null]},           {null}},
        {{"$1::boolean", [true]},           {true}},
        {{"$1::boolean", [false]},          {false}},
        {{"$1::char",    [<<"e">>]},        {<<"e">>}},
        {{"$1::char",    [<<"å"/utf8>>]},   {<<"å"/utf8>>}},
        {{"$1::varchar", [<<"åja"/utf8>>]}, {<<"åja"/utf8>>}},
        {{"$1::text",    [<<"åja"/utf8>>]}, {<<"åja"/utf8>>}},
        {{"$1::integer", [42]},             {42}},
        {{"$1::float4",  [42]},             {42.0}},
        {{"$1::float8",  [42]},             {42.0}},
        {{"$1::float4",  [42.0]},           {42.0}},
        {{"$1::float8",  [42.0]},           {42.0}},
        {{"$1::float4",  ['NaN']},          {'NaN'}},
        {{"$1::float8",  ['NaN']},          {'NaN'}},
        {{"$1::float4",  ['infinity']},     {'infinity'}},
        {{"$1::float8",  ['infinity']},     {'infinity'}},
        {{"$1::float4",  ['-infinity']},    {'-infinity'}},
        {{"$1::float8",  ['-infinity']},    {'-infinity'}},
        {{"$1::bytea",   [<<1, 2, 3>>]},    {<<1, 2, 3>>}}
    ]).

select_uuid_test(Config) ->
    select_test(Config, [
        {"'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::uuid", {<<160, 238, 188, 153, 156, 11, 78, 248, 187, 109, 107, 185, 189, 56, 10, 17>>}}
    ]).

select_array_test(Config) ->
    select_test(Config, [
        {"array[]::integer[]",              {[]}},
        {"array[1]",                        {[1]}},
        {"array[1, 2]",                     {[1, 2]}},
        {"array[[0], [1]]",                 {[[0], [1]]}},
        {"array[array[0]]",                 {[[0]]}},
        {"'{}'::integer[]",                 {[]}},
        {"'{hello}'::varchar[]",            {[<<"hello">>]}}
    ]).

select_time_test(Config) ->
    Cases = [
        {"'00:00:00'::time",                {{0, 0, 0}, 0}},
        {"'01:02:03'::time",                {{1, 2, 3}, 0}},
        {"'23:59:59'::time",                {{23, 59, 59}, 0}},
        {"'00:00:00.123'::time",            {{0, 0, 0}, 123000}},
        {"'00:00:00.123456'::time",         {{0, 0, 0}, 123456}},
        {"'04:05:06+02'::timetz",           {{2, 5, 6}, 0}},
        {"'04:05:06.123456+02'::timetz",    {{2, 5, 6}, 123456}}
    ],
    select_test(Config, lists:map(fun ({Expr, {Time, MicroSeconds}}) ->
        Expect =
            erlang:convert_time_unit(calendar:time_to_seconds(Time), second, native) +
            erlang:convert_time_unit(MicroSeconds, microsecond, native),
        {Expr, {Expect}}
    end, Cases)).

select_record_test(Config) ->
    select_test(Config, [
        {"row(1, '2')",                     {#{1 => 1, 2 => <<"2">>}}},
        {"row(1, '{2}'::integer[])",        {#{1 => 1, 2 => [2]}}}
    ]).

select_oid_test(Config) ->
    select_test(Config, [
        {"4294967295::oid",                         {4294967295}},

        {"'-'::regproc",                            {0}},
        {"'-'::regproc::text",                      {<<"-">>}},

        {"'sum(int4)'::regprocedure",               {2108}},
        {"'sum(int4)'::regprocedure::text",         {<<"sum(integer)">>}},

        {"'pg_catalog.||/'::regoper",               {597}},
        {"'pg_catalog.||/'::regoper::text",         {<<"||/">>}},

        {"'+(integer,integer)'::regoperator",       {551}},
        {"'+(integer,integer)'::regoperator::text", {<<"+(integer,integer)">>}},

        {"'pg_type'::regclass",                     {1247}},
        {"'pg_type'::regclass::text",               {<<"pg_type">>}},

        {"'int4'::regtype",                         {23}},
        {"'int4'::regtype::text",                   {<<"integer">>}},

        {"xmin, xmax from pg_type limit 1",         fun (Result, Comment) ->
            ?assertMatch({Xmin, Xmax} when is_integer(Xmin) andalso is_integer(Xmax), Result, Comment)
        end},
        {"cmin, cmax from pg_type limit 1",         fun (Result, Comment) ->
            ?assertMatch({Cmin, Cmax} when is_integer(Cmin) andalso is_integer(Cmax), Result, Comment)
        end},
        {"pg_current_xact_id()",                    fun (Result, Comment) ->
            ?assertMatch({TransactionID} when is_integer(TransactionID), Result, Comment)
        end}
    ]).


select_void_test(Config) ->
    Connection = proplists:get_value(connection, Config),
    ?assertMatch({ok, #{command := select, rows := 1}, [#{pg_sleep := undefined}]},
        pgc_client:execute(Connection, "select pg_sleep(0.1)", [])).


select_xid8_test(Config) ->
    Connection = proplists:get_value(connection, Config),
    ?assertMatch({ok, #{command := select, rows := 1}, [#{pg_current_xact_id := XID}]} when is_integer(XID),
        pgc_client:execute(Connection, "select pg_current_xact_id()", [])).


select_empty_test(Config) ->
    Connection = proplists:get_value(connection, Config),
    ?assertMatch({ok, #{command := select, rows := 0, columns := [usename]}, []},
        pgc_client:execute(Connection, "select usename from pg_user where false", [])).


select_ltree_test(Config) ->
    select_test(Config, [
        {"'a.b.c'::ltree",                  {<<"a.b.c">>}},
        {"'a.b.*'::lquery",                 {<<"a.b.*">>}}
    ]).

select_interval_test(Config) ->
    select_test(Config, [
        {"'1 day 12 hours 59 min 10 sec'::interval",    {#{microseconds => 46_750_000_000, days => 1, months => 0}}},
        {"'1 year 2 months 1 day'::interval",           {#{microseconds => 0, days => 1, months => 14}}}
    ]).

execute_timeout_test(Config) ->
    Connection = proplists:get_value(connection, Config),
    ?assertMatch({error, #{name := query_canceled}},
        pgc_client:execute(Connection, "select pg_sleep(0.1)", [], #{timeout => 50})),
    ?assertMatch({ok, #{command := select}, [_]},
        pgc_client:execute(Connection, "select pg_sleep(0.1)", [], #{timeout => 500})).


transaction_commit_test(Config) ->
    Connection = proplists:get_value(connection, Config),
    ?assertEqual({ok, 1},
        pgc_client:transaction(Connection, fun () ->
            {ok, #{}, [#{test := Value}]} = pgc_client:execute(Connection, <<"select 1 as test">>, []),
            {ok, Value}
        end, #{})).


transaction_rollback_test(Config) ->
    Connection = proplists:get_value(connection, Config),
    ?assertEqual({error, canceled},
        pgc_client:transaction(Connection, fun () ->
            {ok, #{}, [#{test := 1}]} = pgc_client:execute(Connection, <<"select 1 as test">>, []),
            pgc_client:rollback(Connection, {error, canceled})
        end, #{})).


transaction_error_test(Config) ->
    Connection = proplists:get_value(connection, Config),
    ?assertError(canceled, pgc_client:transaction(Connection, fun () ->
        {ok, #{}, [#{test := 1}]} = pgc_client:execute(Connection, <<"select 1 as test">>, []),
        erlang:error(canceled)
    end, #{})).


hibernate_test(Config) ->
    Connection = proplists:get_value(connection, Config),
    [{memory, M0}] = erlang:process_info(Connection, [memory]),
    {ok, #{command := select}, _} = pgc_client:execute(Connection, <<"select * from pg_user">>, []),
    [{memory, M1}] = erlang:process_info(Connection, [memory]),
    timer:sleep(300),
    ?assertMatch([{current_function, {erlang, hibernate, _}}], erlang:process_info(Connection, [current_function])),
    [{memory, M2}] = erlang:process_info(Connection, [memory]),
    ct:log([
        "Memory usage before query: ~b bytes~n",
        "Memory usage after query: ~b bytes~n",
        "Memory usage after hibernation: ~b bytes"
    ], [M0, M1, M2]),
    ?assert(M2 =< M1).


binref_leak_test(Config) ->
    Connection = proplists:get_value(connection, Config),
    {ok, #{command := select}, _Rows} = pgc_client:execute(Connection, <<"select * from pg_views">>, [], #{}),
    _ = erlang:garbage_collect(Connection),
    [{binary, []}] = erlang:process_info(Connection, [binary]),
    ok.


% ------------------------------------------------------------------------------
% Helpers
% ------------------------------------------------------------------------------

select_test(Config, Cases) ->
    Connection = proplists:get_value(connection, Config),
    lists:foreach(fun({SQLExpr, Expect}) ->
        {Statement, Parameters} = case SQLExpr of
            {SQLExprText, SQLExprParams} -> {["select " | SQLExprText], SQLExprParams};
            SQLExprText -> {["select " | SQLExprText], []}
        end,
        ct:log("Executing statement: ~s", [Statement]),
        {ok, _, [Result]} = pgc_client:execute(Connection, Statement, Parameters, #{row => tuple}),
        case is_function(Expect, 2) of
            true -> Expect(Result, "(" ++ SQLExprText ++ ") result mismatch");
            false -> ?assertEqual(Expect, Result, "(" ++ SQLExprText ++ ") result mismatch")
        end
    end, Cases).
