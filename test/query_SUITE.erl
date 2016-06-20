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
    decode_oid/1
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
            decode_oid
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
    {ok, [_], [{null}]} = execute("SELECT NULL", Config),
    {ok, [_, _], [{true, false}]} = execute("SELECT true, false", Config),
    {ok, [_], [{<<"e">>}]} = execute("SELECT 'e'::char", Config),
    %{ok, [_], [{<<"ẽ">>}]} = execute("SELECT 'ẽ'::char", Config),
    {ok, [_], [{42}]} = execute("SELECT 42", Config),
    {ok, [_], [{42.0}]} = execute("SELECT 42::float", Config),
    {ok, [_], [{'NaN'}]} = execute("SELECT 'NaN'::float", Config),
    {ok, [_], [{infinity}]} = execute("SELECT 'inf'::float", Config),
    {ok, [_], [{'-infinity'}]} = execute("SELECT '-inf'::float", Config),
    %{ok, [_], [{<<"ẽric">>}]} = execute("SELECT 'ẽric'", Config),
    %{ok, [_], [{<<"ẽric">>}]} = execute("SELECT 'ẽric'::varchar", Config),
    {ok, [_], [{<<1, 2, 3>>}]} = execute("SELECT '\\001\\002\\003'::bytea", Config).

decode_uuid(Config) ->
    UUID = <<160, 238, 188, 153, 156, 11, 78, 248, 187, 109, 107, 185, 189, 56, 10, 17>>,
    {ok, [_], [{UUID}]} = execute("SELECT 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::uuid", Config).

decode_arrays(Config) ->
    {ok, [_], [{[]}]} = execute("SELECT ARRAY[]::integer[]", Config),
    {ok, [_], [{[1]}]} = execute("SELECT ARRAY[1]", Config),
    {ok, [_], [{[1, 2]}]} = execute("SELECT ARRAY[1, 2]", Config),
    {ok, [_], [{[[0], [1]]}]} = execute("SELECT ARRAY[[0], [1]]", Config),
    {ok, [_], [{[[0]]}]} = execute("SELECT ARRAY[ARRAY[0]]", Config).

decode_time(Config) ->
    {ok, [_], [{#pgsql_time{hours = 0, minutes = 0, seconds = 0, micro_seconds = 0}}]}
        = execute("SELECT time '00:00:00'", Config),
    {ok, [_], [{#pgsql_time{hours = 1, minutes = 2, seconds = 3, micro_seconds = 0}}]}
        = execute("SELECT time '01:02:03'", Config),
    {ok, [_], [{#pgsql_time{hours = 23, minutes = 59, seconds = 59, micro_seconds = 0}}]}
        = execute("SELECT time '23:59:59'", Config),
    {ok, [_], [{#pgsql_time{hours = 0, minutes = 0, seconds = 0, micro_seconds = 123000}}]}
        = execute("SELECT time '00:00:00.123'", Config),
    {ok, [_], [{#pgsql_time{hours = 0, minutes = 0, seconds = 0, micro_seconds = 123456}}]}
        = execute("SELECT time '00:00:00.123456'", Config),
    {ok, [_], [{#pgsql_time{hours = 1, minutes = 2, seconds = 3, micro_seconds = 123456}}]}
        = execute("SELECT time '01:02:03.123456'", Config),
    {ok, [_], [{#pgsql_time{hours = 2, minutes = 5, seconds = 6, micro_seconds = 0}}]}
        = execute("SELECT timetz '04:05:06+02'", Config).

decode_date(Config) ->
    {ok, [_], [{#pgsql_date{year = 1, month = 1, day = 1}}]} =
        execute("SELECT date '0001-01-01'", Config),
    {ok, [_], [{#pgsql_date{year = 1, month = 2, day = 3}}]} =
        execute("SELECT date '0001-02-03'", Config),
    {ok, [_], [{#pgsql_date{year = 2013, month = 9, day = 23}}]} =
        execute("SELECT date '2013-09-23'", Config).

decode_datetime(Config) ->
    {ok, [_], [{#pgsql_datetime{year = 2001, month = 1, day = 1, hours = 0, minutes = 0, seconds = 0, micro_seconds = 0}}]}
        = execute("SELECT timestamp '2001-01-01 00:00:00'", Config),
    {ok, [_], [{#pgsql_datetime{year = 2013, month = 9, day = 23, hours = 14, minutes = 4, seconds = 37, micro_seconds = 123000}}]}
        = execute("SELECT timestamp '2013-09-23 14:04:37.123'", Config),
    {ok, [_], [{#pgsql_datetime{year = 2013, month = 9, day = 23, hours = 14, minutes = 4, seconds = 37, micro_seconds = 0}}]}
        = execute("SELECT timestamp '2013-09-23 14:04:37 PST'", Config),
    {ok, [_], [{#pgsql_datetime{year = 1, month = 1, day = 1, hours = 0, minutes = 0, seconds = 0, micro_seconds = 123456}}]}
        = execute("SELECT timestamp '0001-01-01 00:00:00.123456'", Config).

decode_interval(Config) ->
    {ok, [_], [{#pgsql_interval{months = 0, days = 0, micro_seconds = 0}}]}
        = execute("SELECT interval '0'", Config),
    {ok, [_], [{#pgsql_interval{months = 100, days = 0, micro_seconds = 0}}]}
        = execute("SELECT interval '100 months'", Config),
    {ok, [_], [{#pgsql_interval{months = 0, days = 100, micro_seconds = 0}}]}
        = execute("SELECT interval '100 days'", Config),
    {ok, [_], [{#pgsql_interval{months = 0, days = 0, micro_seconds = 100000000}}]}
        = execute("SELECT interval '100 seconds'", Config),
    {ok, [_], [{#pgsql_interval{months = 14, days = 40, micro_seconds = 10920000000}}]}
        = execute("SELECT interval '1 year 2 months 40 days 3 hours 2 minutes'", Config).

decode_record(Config) ->
    {ok, [_], [{#{<<"f1">> := 1, <<"f2">> := <<"2">>}}]}
        = execute("SELECT row(1, '2')", Config),
    {ok, [_], [{[#{<<"f1">> := 1, <<"f2">> := <<"2">>}]}]}
        = execute("SELECT ARRAY[row(1, '2')]", Config).

decode_oid(Config) ->
    {ok, [_], [{4294967295}]} = execute("select 4294967295::oid;", Config),

    {ok, [_], [{<<"-">>}]} = execute("select '-'::regproc::text;", Config),
    {ok, [_], [{<<"sum(integer)">>}]} = execute("select 'sum(int4)'::regprocedure::text;", Config),
    {ok, [_], [{<<"||/">>}]} = execute("select 'pg_catalog.||/'::regoper::text;", Config),
    {ok, [_], [{<<"+(integer,integer)">>}]} = execute("select '+(integer,integer)'::regoperator::text;", Config),
    {ok, [_], [{<<"pg_type">>}]} = execute("select 'pg_type'::regclass::text;", Config),
    {ok, [_], [{<<"integer">>}]} = execute("select 'int4'::regtype::text;", Config),

    {ok, [_], [{0}]} = execute("select '-'::regproc;", Config),
    {ok, [_], [{44}]} = execute("select 'regprocin'::regproc;", Config),
    {ok, [_], [{2108}]} = execute("select 'sum(int4)'::regprocedure;", Config),
    {ok, [_], [{597}]} = execute("select 'pg_catalog.||/'::regoper;", Config),
    {ok, [_], [{551}]} = execute("select '+(integer,integer)'::regoperator;", Config),
    {ok, [_], [{1247}]} = execute("select 'pg_type'::regclass;", Config),
    {ok, [_], [{23}]} = execute("select 'int4'::regtype;", Config),

    {ok, [_, _], [{Xmin, Xmax}]} = execute("select xmin, xmax from pg_type limit 1;", Config),
    true = is_number(Xmin) and is_number(Xmax),

    {ok, [_, _], [{Cmin, Cmax}]} = execute("select cmin, cmax from pg_type limit 1;", Config),
    true = is_number(Cmin) and is_number(Cmax).

%% Helpers
execute(Query, Config) ->
    execute(Query, [], #{}, Config).

execute(Query, Parameters, Opts, Config) ->
    pgsql_connection:execute(?config(conn, Config), Query, Parameters, Opts).
