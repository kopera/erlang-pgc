-module(query_SUITE).
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
    decode_basic/1,
    decode_uuid/1,
    decode_arrays/1,
    decode_time/1,
    % decode_date/1,
    % decode_datetime/1,
    % decode_interval/1,
    decode_record/1,
    decode_oid/1
    % decode_network/1
]).

-define(ASSERT, true).
-include_lib("stdlib/include/assert.hrl").


suite() ->
    [
        {require, address, 'postgresql_server_address'},
        {require, user, 'postgresql_server_user'},
        {require, password, 'postgresql_server_password'},
        {require, database, 'postgresql_server_database'}
    ].

all() ->
    [{group, decode}].

groups() ->
    [
        {decode, [parallel, shuffle], [
            decode_basic,
            decode_uuid,
            decode_arrays,
            decode_time,
            % decode_date,
            % decode_datetime,
            % decode_interval,
            decode_record,
            decode_oid
            % decode_network
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

%% Decode

decode_basic(Config) ->
    Connection = proplists:get_value(connection, Config),
    {ok, _, [{null}]} = execute(Connection, "select null"),
    {ok, _, [{true, false}]} = execute(Connection, "select true, false"),
    {ok, _, [{<<"e">>}]} = execute(Connection, "select 'e'::char"),
    {ok, _, [{<<"å"/utf8>>}]} = execute(Connection, "select 'å'::char"),
    {ok, _, [{<<"åja"/utf8>>}]} = execute(Connection, "select 'åja'"),
    {ok, _, [{42}]} = execute(Connection, "select 42"),
    {ok, _, [{42.0}]} = execute(Connection, "select 42::float"),
    {ok, _, [{'NaN'}]} = execute(Connection, "select 'NaN'::float"),
    {ok, _, [{infinity}]} = execute(Connection, "select 'inf'::float"),
    {ok, _, [{'-infinity'}]} = execute(Connection, "select '-inf'::float"),
    {ok, _, [{'-infinity'}]} = execute(Connection, "select '-inf'::float"),
    {ok, _, [{<<1, 2, 3>>}]} = execute(Connection, "SELECT '\\001\\002\\003'::bytea").


decode_uuid(Config) ->
    Connection = proplists:get_value(connection, Config),
    UUID = <<160, 238, 188, 153, 156, 11, 78, 248, 187, 109, 107, 185, 189, 56, 10, 17>>,
    {ok, _, [{UUID}]} = execute(Connection, "SELECT 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::uuid").

decode_arrays(Config) ->
    Connection = proplists:get_value(connection, Config),
    lists:foreach(fun ({SQLExpr, Expect}) ->
        {ok, _, [{Result}]} = execute(Connection, ["select " | SQLExpr]),
        ?assertEqual(Expect, Result, "(" ++ SQLExpr ++ ") result mismatch")
    end, [
        {"array[]::integer[]",          []},
        {"array[1]",                    [1]},
        {"array[1, 2]",                 [1, 2]},
        {"array[[0], [1]]",             [[0], [1]]},
        {"array[array[0]]",             [[0]]},
        {"'{}'::integer[]",             []},
        {"'{hello}'::varchar[]",        [<<"hello">>]}
    ]).

decode_time(Config) ->
    Connection = proplists:get_value(connection, Config),
    lists:foreach(fun ({SQLExpr, {Time, MicroSeconds}}) ->
        Expect =
            erlang:convert_time_unit(calendar:time_to_seconds(Time), second, native) +
            erlang:convert_time_unit(MicroSeconds, microsecond, native),
        {ok, _, [{Result}]} = execute(Connection, ["select " | SQLExpr]),
        ?assertEqual(Expect, Result, "(" ++ SQLExpr ++ ") result mismatch")
    end, [
        {"'00:00:00'::time",             {{0, 0, 0}, 0}},
        {"'01:02:03'::time",             {{1, 2, 3}, 0}},
        {"'23:59:59'::time",             {{23, 59, 59}, 0}},
        {"'00:00:00.123'::time",         {{0, 0, 0}, 123000}},
        {"'00:00:00.123456'::time",      {{0, 0, 0}, 123456}},
        {"'04:05:06+02'::timetz",        {{2, 5, 6}, 0}},
        {"'04:05:06.123456+02'::timetz", {{2, 5, 6}, 123456}}
    ]).

% decode_date(Config) ->
%     #{rows := [{#pgsql_date{year = 1, month = 1, day = 1}}]}
%         = execute("SELECT date '0001-01-01'", Config),
%     #{rows := [{#pgsql_date{year = 1, month = 2, day = 3}}]}
%         = execute("SELECT date '0001-02-03'", Config),
%     #{rows := [{#pgsql_date{year = 2013, month = 9, day = 23}}]}
%         = execute("SELECT date '2013-09-23'", Config).

% decode_datetime(Config) ->
%     #{rows := [{#pgsql_datetime{year = 2001, month = 1, day = 1, hour = 0, minute = 0, second = 0, microsecond = 0}}]}
%         = execute("SELECT timestamp '2001-01-01 00:00:00'", Config),
%     #{rows := [{#pgsql_datetime{year = 2013, month = 9, day = 23, hour = 14, minute = 4, second = 37, microsecond = 123000}}]}
%         = execute("SELECT timestamp '2013-09-23 14:04:37.123'", Config),
%     #{rows := [{#pgsql_datetime{year = 2013, month = 9, day = 23, hour = 14, minute = 4, second = 37, microsecond = 0}}]}
%         = execute("SELECT timestamp '2013-09-23 14:04:37 PST'", Config),
%     #{rows := [{#pgsql_datetime{year = 1, month = 1, day = 1, hour = 0, minute = 0, second = 0, microsecond = 123456}}]}
%         = execute("SELECT timestamp '0001-01-01 00:00:00.123456'", Config).

% decode_interval(Config) ->
%     #{rows := [{#pgsql_interval{months = 0, days = 0, microseconds = 0}}]}
%         = execute("SELECT interval '0'", Config),
%     #{rows := [{#pgsql_interval{months = 100, days = 0, microseconds = 0}}]}
%         = execute("SELECT interval '100 months'", Config),
%     #{rows := [{#pgsql_interval{months = 0, days = 100, microseconds = 0}}]}
%         = execute("SELECT interval '100 days'", Config),
%     #{rows := [{#pgsql_interval{months = 0, days = 0, microseconds = 100000000}}]}
%         = execute("SELECT interval '100 seconds'", Config),
%     #{rows := [{#pgsql_interval{months = 14, days = 40, microseconds = 10920000000}}]}
%         = execute("SELECT interval '1 year 2 months 40 days 3 hours 2 minutes'", Config).

decode_record(Config) ->
    Connection = proplists:get_value(connection, Config),
    {ok, _, [{#{1 := 1, 2 := <<"2">>}}]} = execute(Connection, "select row(1, '2')"),
    {ok, _, [{[#{1 := 1, 2 := <<"2">>}]}]} = execute(Connection, "select array[row(1, '2')]").

decode_oid(Config) ->
    Connection = proplists:get_value(connection, Config),
    {ok, _, [{4294967295}]} = execute(Connection, "select 4294967295::oid"),

    {ok, _, [{<<"-">>}]} = execute(Connection, "select '-'::regproc::text"),
    {ok, _, [{<<"sum(integer)">>}]} = execute(Connection, "select 'sum(int4)'::regprocedure::text"),
    {ok, _, [{<<"||/">>}]} = execute(Connection, "select 'pg_catalog.||/'::regoper::text"),
    {ok, _, [{<<"+(integer,integer)">>}]} = execute(Connection, "select '+(integer,integer)'::regoperator::text"),
    {ok, _, [{<<"pg_type">>}]} = execute(Connection, "select 'pg_type'::regclass::text"),
    {ok, _, [{<<"integer">>}]} = execute(Connection, "select 'int4'::regtype::text"),

    {ok, _, [{0}]} = execute(Connection, "select '-'::regproc"),
    {ok, _, [{44}]} = execute(Connection, "select 'regprocin'::regproc"),
    {ok, _, [{2108}]} = execute(Connection, "select 'sum(int4)'::regprocedure"),
    {ok, _, [{597}]} = execute(Connection, "select 'pg_catalog.||/'::regoper"),
    {ok, _, [{551}]} = execute(Connection, "select '+(integer,integer)'::regoperator"),
    {ok, _, [{1247}]} = execute(Connection, "select 'pg_type'::regclass"),
    {ok, _, [{23}]} = execute(Connection, "select 'int4'::regtype"),

    {ok, _, [{Xmin, Xmax}]} = execute(Connection, "select xmin, xmax from pg_type limit 1"),
    true = is_number(Xmin) and is_number(Xmax),

    {ok, _, [{Cmin, Cmax}]} = execute(Connection, "select cmin, cmax from pg_type limit 1"),
    true = is_number(Cmin) and is_number(Cmax).

% decode_network(Config) ->
%     #{rows := [{#pgsql_inet{address = {127, 0, 0, 1}}}]}
%         = execute("select '127.0.0.1'::inet", Config),
%     #{rows := [{#pgsql_inet{address = {8193, 43981, 0, 0, 0, 0, 0, 0}}}]}
%         = execute("select '2001:abcd::'::inet", Config),
%     #{rows := [{#pgsql_cidr{address = {127, 0, 0, 1}, mask = 32}}]}
%         = execute("select '127.0.0.1/32'::cidr", Config),
%     #{rows := [{#pgsql_cidr{address = {8193, 43981, 0, 0, 0, 0, 0, 0}, mask = 128}}]}
%         = execute("select '2001:abcd::/128'::cidr", Config),
%     #{rows := [{#pgsql_macaddr{address = {8, 1, 43, 5, 7, 9}}}]}
%         = execute("select '08:01:2b:05:07:09'::macaddr", Config).

% ------------------------------------------------------------------------------
% Helpers
% ------------------------------------------------------------------------------

%% @private
execute(Connection, Statement) ->
    pgc:execute(Connection, {Statement, []}, #{row => tuple}).