-module(query_SUITE).
-export([
    all/0,
    groups/0,
    init_per_suite/1,
    end_per_suite/1,
    init_per_testcase/2,
    end_per_testcase/2
]).

-export([
    decode_basic_types/1,
    decode_uuid/1,
    decode_arrays/1,
    decode_time/1,
    decode_date/1,
    decode_datetime/1,
    decode_interval/1,
    decode_record/1,
    decode_oid/1,
    decode_network/1
]).

-include_lib("common_test/include/ct.hrl").
-include("../include/types.hrl").

all() ->
    [{group, decode}].

groups() ->
    [
        {decode, [parallel, shuffle], [
            decode_basic_types,
            decode_uuid,
            decode_arrays,
            decode_time,
            decode_date,
            decode_datetime,
            decode_interval,
            decode_record,
            decode_oid,
            decode_network
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

%% Decode

decode_basic_types(Config) ->
    #{rows := [{null}]} = execute("SELECT NULL", Config),
    #{rows := [{true, false}]} = execute("SELECT true, false", Config),
    #{rows := [{<<"e">>}]} = execute("SELECT 'e'::char", Config),
    %#{rows := [{<<"ẽ">>}]} = execute("SELECT 'ẽ'::char", Config),
    #{rows := [{42}]} = execute("SELECT 42", Config),
    #{rows := [{42.0}]} = execute("SELECT 42::float", Config),
    #{rows := [{'NaN'}]} = execute("SELECT 'NaN'::float", Config),
    #{rows := [{infinity}]} = execute("SELECT 'inf'::float", Config),
    #{rows := [{'-infinity'}]} = execute("SELECT '-inf'::float", Config),
    %#{rows := [{<<"ẽric">>}]} = execute("SELECT 'ẽric'", Config),
    %#{rows := [{<<"ẽric">>}]} = execute("SELECT 'ẽric'::varchar", Config),
    #{rows := [{<<1, 2, 3>>}]} = execute("SELECT '\\001\\002\\003'::bytea", Config).

decode_uuid(Config) ->
    UUID = <<160, 238, 188, 153, 156, 11, 78, 248, 187, 109, 107, 185, 189, 56, 10, 17>>,
    #{rows := [{UUID}]} = execute("SELECT 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::uuid", Config).

decode_arrays(Config) ->
    #{rows := [{[]}]} = execute("SELECT ARRAY[]::integer[]", Config),
    #{rows := [{[1]}]} = execute("SELECT ARRAY[1]", Config),
    #{rows := [{[1, 2]}]} = execute("SELECT ARRAY[1, 2]", Config),
    #{rows := [{[[0], [1]]}]} = execute("SELECT ARRAY[[0], [1]]", Config),
    #{rows := [{[[0]]}]} = execute("SELECT ARRAY[ARRAY[0]]", Config).

decode_time(Config) ->
    #{rows := [{#pgsql_time{hour = 0, minute = 0, second = 0, microsecond = 0}}]}
        = execute("SELECT time '00:00:00'", Config),
    #{rows := [{#pgsql_time{hour = 1, minute = 2, second = 3, microsecond = 0}}]}
        = execute("SELECT time '01:02:03'", Config),
    #{rows := [{#pgsql_time{hour = 23, minute = 59, second = 59, microsecond = 0}}]}
        = execute("SELECT time '23:59:59'", Config),
    #{rows := [{#pgsql_time{hour = 0, minute = 0, second = 0, microsecond = 123000}}]}
        = execute("SELECT time '00:00:00.123'", Config),
    #{rows := [{#pgsql_time{hour = 0, minute = 0, second = 0, microsecond = 123456}}]}
        = execute("SELECT time '00:00:00.123456'", Config),
    #{rows := [{#pgsql_time{hour = 1, minute = 2, second = 3, microsecond = 123456}}]}
        = execute("SELECT time '01:02:03.123456'", Config),
    #{rows := [{#pgsql_time{hour = 2, minute = 5, second = 6, microsecond = 0}}]}
        = execute("SELECT timetz '04:05:06+02'", Config).

decode_date(Config) ->
    #{rows := [{#pgsql_date{year = 1, month = 1, day = 1}}]}
        = execute("SELECT date '0001-01-01'", Config),
    #{rows := [{#pgsql_date{year = 1, month = 2, day = 3}}]}
        = execute("SELECT date '0001-02-03'", Config),
    #{rows := [{#pgsql_date{year = 2013, month = 9, day = 23}}]}
        = execute("SELECT date '2013-09-23'", Config).

decode_datetime(Config) ->
    #{rows := [{#pgsql_datetime{year = 2001, month = 1, day = 1, hour = 0, minute = 0, second = 0, microsecond = 0}}]}
        = execute("SELECT timestamp '2001-01-01 00:00:00'", Config),
    #{rows := [{#pgsql_datetime{year = 2013, month = 9, day = 23, hour = 14, minute = 4, second = 37, microsecond = 123000}}]}
        = execute("SELECT timestamp '2013-09-23 14:04:37.123'", Config),
    #{rows := [{#pgsql_datetime{year = 2013, month = 9, day = 23, hour = 14, minute = 4, second = 37, microsecond = 0}}]}
        = execute("SELECT timestamp '2013-09-23 14:04:37 PST'", Config),
    #{rows := [{#pgsql_datetime{year = 1, month = 1, day = 1, hour = 0, minute = 0, second = 0, microsecond = 123456}}]}
        = execute("SELECT timestamp '0001-01-01 00:00:00.123456'", Config).

decode_interval(Config) ->
    #{rows := [{#pgsql_interval{months = 0, days = 0, microseconds = 0}}]}
        = execute("SELECT interval '0'", Config),
    #{rows := [{#pgsql_interval{months = 100, days = 0, microseconds = 0}}]}
        = execute("SELECT interval '100 months'", Config),
    #{rows := [{#pgsql_interval{months = 0, days = 100, microseconds = 0}}]}
        = execute("SELECT interval '100 days'", Config),
    #{rows := [{#pgsql_interval{months = 0, days = 0, microseconds = 100000000}}]}
        = execute("SELECT interval '100 seconds'", Config),
    #{rows := [{#pgsql_interval{months = 14, days = 40, microseconds = 10920000000}}]}
        = execute("SELECT interval '1 year 2 months 40 days 3 hours 2 minutes'", Config).

decode_record(Config) ->
    #{rows := [{{1, <<"2">>}}]} = execute("SELECT row(1, '2')", Config),
    #{rows := [{[{1, <<"2">>}]}]} = execute("SELECT ARRAY[row(1, '2')]", Config).

decode_oid(Config) ->
    #{rows := [{4294967295}]} = execute("select 4294967295::oid;", Config),

    #{rows := [{<<"-">>}]} = execute("select '-'::regproc::text;", Config),
    #{rows := [{<<"sum(integer)">>}]} = execute("select 'sum(int4)'::regprocedure::text;", Config),
    #{rows := [{<<"||/">>}]} = execute("select 'pg_catalog.||/'::regoper::text;", Config),
    #{rows := [{<<"+(integer,integer)">>}]} = execute("select '+(integer,integer)'::regoperator::text;", Config),
    #{rows := [{<<"pg_type">>}]} = execute("select 'pg_type'::regclass::text;", Config),
    #{rows := [{<<"integer">>}]} = execute("select 'int4'::regtype::text;", Config),

    #{rows := [{0}]} = execute("select '-'::regproc;", Config),
    #{rows := [{44}]} = execute("select 'regprocin'::regproc;", Config),
    #{rows := [{2108}]} = execute("select 'sum(int4)'::regprocedure;", Config),
    #{rows := [{597}]} = execute("select 'pg_catalog.||/'::regoper;", Config),
    #{rows := [{551}]} = execute("select '+(integer,integer)'::regoperator;", Config),
    #{rows := [{1247}]} = execute("select 'pg_type'::regclass;", Config),
    #{rows := [{23}]} = execute("select 'int4'::regtype;", Config),

    #{rows := [{Xmin, Xmax}]} = execute("select xmin, xmax from pg_type limit 1;", Config),
    true = is_number(Xmin) and is_number(Xmax),

    #{rows := [{Cmin, Cmax}]} = execute("select cmin, cmax from pg_type limit 1;", Config),
    true = is_number(Cmin) and is_number(Cmax).

decode_network(Config) ->
    #{rows := [{#pgsql_inet{address = {127, 0, 0, 1}}}]}
        = execute("select '127.0.0.1'::inet", Config),
    #{rows := [{#pgsql_inet{address = {8193, 43981, 0, 0, 0, 0, 0, 0}}}]}
        = execute("select '2001:abcd::'::inet", Config),
    #{rows := [{#pgsql_cidr{address = {127, 0, 0, 1}, mask = 32}}]}
        = execute("select '127.0.0.1/32'::cidr", Config),
    #{rows := [{#pgsql_cidr{address = {8193, 43981, 0, 0, 0, 0, 0, 0}, mask = 128}}]}
        = execute("select '2001:abcd::/128'::cidr", Config),
    #{rows := [{#pgsql_macaddr{address = {8, 1, 43, 5, 7, 9}}}]}
        = execute("select '08:01:2b:05:07:09'::macaddr", Config).

%% Helpers
execute(Query, Config) ->
    execute(Query, [], #{}, Config).

execute(Query, Parameters, Opts, Config) ->
    pgsql_connection:execute(?config(conn, Config), Query, Parameters, Opts).
