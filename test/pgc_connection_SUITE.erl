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
    select_oid_test/1
]).
-export([
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
        hibernate_after => 500
    },
    {ok, Connection} = pgc_connection:start_link(TransportOptions, ConnectionOptions, self()),
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
        hibernate_test,
        binref_leak_test
    ].

%% @doc https://www.erlang.org/doc/man/ct_suite#Module:groups-0
groups() ->
    [
        {select, [parallel, shuffle], [
            select_basic_test,
            select_uuid_test,
            select_array_test,
            select_time_test,
            select_record_test,
            select_oid_test
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
        {"'åja'",                           {<<"åja"/utf8>>}},
        {"42",                              {42}},
        {"42::float",                       {42.0}},
        {"'NaN'::float",                    {'NaN'}},
        {"'inf'::float",                    {'infinity'}},
        {"'-inf'::float",                   {'-infinity'}},
        {"'\\001\\002\\003'::bytea",        {<<1, 2, 3>>}}
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


% timeout(Config) ->
%     {error, timeout} = execute("select pg_sleep(0.1)", [], #{timeout => 50}, Config),
%     #{rows := [{void}]} = execute("select pg_sleep(0.1)", [], #{timeout => 200}, Config).


hibernate_test(Config) ->
    Connection = proplists:get_value(connection, Config),
    [{memory, M0}] = erlang:process_info(Connection, [memory]),
    {ok, #{command := select}, _} = execute(Connection, <<"select * from pg_user">>),
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


binref_leak_test(Config) ->
    Connection = proplists:get_value(connection, Config),
    {ok, #{command := select}, _Rows} = execute(Connection, <<"select * from pg_views">>),
    _ = erlang:garbage_collect(Connection),
    [{binary, []}] = erlang:process_info(Connection, [binary]),
    ok.


% ------------------------------------------------------------------------------
% Helpers
% ------------------------------------------------------------------------------

execute(Connection, Statement) ->
    pgc_connection:execute(Connection, {Statement, []}).


select_test(Config, Cases) ->
    Connection = proplists:get_value(connection, Config),
    lists:foreach(fun({SQLExpr, Expect}) ->
        Statement = ["select " | SQLExpr],
        ct:log("Executing statement: ~s", [Statement]),
        {ok, _, [Result]} = pgc_connection:execute(Connection, {Statement, []}, #{row => tuple}),
        case is_function(Expect, 2) of
            true -> Expect(Result, "(" ++ SQLExpr ++ ") result mismatch");
            false -> ?assertEqual(Expect, Result, "(" ++ SQLExpr ++ ") result mismatch")
        end
    end, Cases).