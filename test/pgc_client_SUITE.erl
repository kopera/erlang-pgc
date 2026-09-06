-module(pgc_client_SUITE).
-moduledoc false.

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
    execute_with_parameters_test/1,
    execute_error_is_not_fatal_test/1,
    execute_row_formats_test/1,
    execute_timeout_cancels_query_test/1,
    execute_streams_rows_test/1,
    execute_halt_cancels_query_test/1,
    execute_resolves_types_across_statements_test/1,
    execute_resolves_type_created_mid_session_test/1,
    transaction_commit_test/1,
    transaction_rollback_test/1,
    transaction_exception_rolls_back_test/1
]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-define(POSTGRES_IMAGE, "postgres:18-alpine").
-define(POSTGRES_USER, "postgres").
-define(POSTGRES_PASSWORD, "postgres").
-define(POSTGRES_DATABASE, "postgres").


-doc false.
suite() ->
    [{timetrap, {seconds, 60}}].

-doc false.
init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(pgc),
    {ok, _} = application:ensure_all_started(erlexec),
    % See pgc_connection_SUITE:init_per_suite/1 for why this is needed on a plain
    % `erl` test node (OTP 29 native cross-module records + lazy code loading).
    {ok, Modules} = application:get_key(pgc, modules),
    lists:foreach(fun code:ensure_loaded/1, Modules),
    Config.

-doc false.
end_per_suite(_Config) ->
    ok = application:stop(pgc),
    ok.

-doc false.
init_per_testcase(_Case, Config) ->
    {Name, Pid, Port} = start_postgres(),
    [{postgres_name, Name}, {postgres_pid, Pid}, {postgres_port, Port} | Config].

-doc false.
end_per_testcase(_Case, Config) ->
    stop_postgres(?config(postgres_pid, Config)),
    ok.

-doc false.
all() ->
    [{group, parallel_tests}].

-doc false.
groups() ->
    [
        {parallel_tests, [parallel], [
            execute_with_parameters_test,
            execute_error_is_not_fatal_test,
            execute_row_formats_test,
            execute_timeout_cancels_query_test,
            execute_streams_rows_test,
            execute_halt_cancels_query_test,
            execute_resolves_types_across_statements_test,
            execute_resolves_type_created_mid_session_test,
            transaction_commit_test,
            transaction_rollback_test,
            transaction_exception_rolls_back_test
        ]}
    ].


% ------------------------------------------------------------------------------
% Test cases
% ------------------------------------------------------------------------------

execute_with_parameters_test(Config) ->
    {ok, Connection} = pgc_client:start_link(connection_options(Config, #{})),

    {ok, Metadata, Rows} = pgc_client:execute(Connection, "select $1::int4 as n, $2::text as t", [<<"42">>, <<"hi">>]),
    ?assertMatch(#{command := ~"select", rows := 1}, Metadata),
    ?assertEqual([#{<<"n">> => <<"42">>, <<"t">> => <<"hi">>}], Rows),

    % The connection reuses the same unnamed statement slot on every call --
    % prove it's still usable for a second, differently-shaped query.
    {ok, _Metadata2, Rows2} = pgc_client:execute(Connection, "select 1 as a, 2 as b, 3 as c", []),
    ?assertEqual([#{<<"a">> => <<"1">>, <<"b">> => <<"2">>, <<"c">> => <<"3">>}], Rows2),

    ok = pgc_client:stop(Connection).

execute_error_is_not_fatal_test(Config) ->
    {ok, Connection} = pgc_client:start_link(connection_options(Config, #{})),

    {error, Error} = pgc_client:execute(Connection, "select * from this_table_does_not_exist", []),
    ?assertMatch(#{code := <<"42P01">>}, Error),
    ?assert(is_process_alive(Connection)),

    % Not fatal -- the connection is still usable afterward.
    {ok, _Metadata, [#{<<"n">> := <<"1">>}]} = pgc_client:execute(Connection, "select 1 as n", []),

    ok = pgc_client:stop(Connection).

execute_row_formats_test(Config) ->
    {ok, Connection} = pgc_client:start_link(connection_options(Config, #{})),

    {ok, _, [[<<"1">>, <<"2">>]]} = pgc_client:execute(Connection, "select 1 as a, 2 as b", [], #{row => list}),
    {ok, _, [{<<"1">>, <<"2">>}]} = pgc_client:execute(Connection, "select 1 as a, 2 as b", [], #{row => tuple}),
    {ok, _, [[{<<"a">>, <<"1">>}, {<<"b">>, <<"2">>}]]} = pgc_client:execute(Connection, "select 1 as a, 2 as b", [], #{row => proplist}),

    ok = pgc_client:stop(Connection).

execute_timeout_cancels_query_test(Config) ->
    {ok, Connection} = pgc_client:start_link(connection_options(Config, #{})),

    ?assertExit({timeout, _}, pgc_client:execute(Connection, "select pg_sleep(10)", [], #{timeout => 200})),

    % If cancellation actually reached Postgres, the connection is free again almost
    % immediately -- without it, this would block for the remaining ~9.8s of the sleep.
    {Time, {ok, _, [#{<<"n">> := <<"1">>}]}} = timer:tc(fun () ->
        pgc_client:execute(Connection, "select 1 as n", [])
    end),
    ?assert(Time < 2_000_000),

    ok = pgc_client:stop(Connection).

execute_streams_rows_test(Config) ->
    {ok, Connection} = pgc_client:start_link(connection_options(Config, #{})),

    Fun = fun (_RowDescription, [N], Acc) -> {cont, [N | Acc]} end,
    {ok, Metadata, Values} = pgc_client:execute(Connection, "select generate_series(1, 5) as n", [], Fun, [], #{}),
    ?assertMatch(#{command := ~"select", rows := 5}, Metadata),
    ?assertEqual([<<"5">>, <<"4">>, <<"3">>, <<"2">>, <<"1">>], Values),

    ok = pgc_client:stop(Connection).

execute_halt_cancels_query_test(Config) ->
    {ok, Connection} = pgc_client:start_link(connection_options(Config, #{})),

    Fun = fun (_RowDescription, [N], Acc) ->
        case N of
            <<"3">> -> {halt, [N | Acc]};
            _ -> {cont, [N | Acc]}
        end
    end,
    {ok, _Metadata, Values} = pgc_client:execute(Connection, "select generate_series(1, 1000000) as n", [], Fun, [], #{}),
    ?assertEqual([<<"3">>, <<"2">>, <<"1">>], Values),

    % Cancellation should free the connection quickly rather than draining a million rows.
    {Time, {ok, _, [#{<<"n">> := <<"1">>}]}} = timer:tc(fun () ->
        pgc_client:execute(Connection, "select 1 as n", [])
    end),
    ?assert(Time < 2_000_000),

    ok = pgc_client:stop(Connection).

execute_resolves_types_across_statements_test(Config) ->
    {ok, Connection} = pgc_client:start_link(connection_options(Config, #{})),

    % A connection's type cache starts empty, so the first execute always exercises the
    % refresh-and-retry path; a second, differently-typed one right after must also succeed.
    {ok, _, [#{<<"n">> := <<"1">>}]} = pgc_client:execute(Connection, "select 1 as n", []),
    {ok, _, [#{<<"t">> := <<"hi">>}]} = pgc_client:execute(Connection, "select 'hi'::text as t", []),

    ok = pgc_client:stop(Connection).

execute_resolves_type_created_mid_session_test(Config) ->
    {ok, Connection} = pgc_client:start_link(connection_options(Config, #{})),

    % Warm the cache before the type below exists, so referencing it later can only succeed
    % if a miss triggers a fresh refresh rather than relying on a one-time startup snapshot.
    {ok, _, [#{<<"n">> := <<"1">>}]} = pgc_client:execute(Connection, "select 1 as n", []),

    {ok, _, []} = pgc_client:execute(Connection, "create type mood as enum ('sad', 'ok', 'happy')", []),
    {ok, _, [#{<<"m">> := <<"happy">>}]} = pgc_client:execute(Connection, "select 'happy'::mood as m", []),

    ok = pgc_client:stop(Connection).

transaction_commit_test(Config) ->
    {ok, Connection} = pgc_client:start_link(connection_options(Config, #{})),

    {ok, _, []} = pgc_client:execute(Connection, "create table pgc_client_test (id int4)", []),

    Result = pgc_client:transaction(Connection, fun () ->
        {ok, _, []} = pgc_client:execute(Connection, "insert into pgc_client_test (id) values (1)", []),
        committed
    end, #{}),
    ?assertEqual(committed, Result),

    {ok, _, [#{<<"count">> := <<"1">>}]} = pgc_client:execute(Connection, "select count(*) as count from pgc_client_test", []),

    ok = pgc_client:stop(Connection).

transaction_rollback_test(Config) ->
    {ok, Connection} = pgc_client:start_link(connection_options(Config, #{})),

    {ok, _, []} = pgc_client:execute(Connection, "create table pgc_client_test (id int4)", []),

    Result = pgc_client:transaction(Connection, fun () ->
        {ok, _, []} = pgc_client:execute(Connection, "insert into pgc_client_test (id) values (1)", []),
        pgc_client:rollback(Connection, rolled_back)
    end, #{}),
    ?assertEqual(rolled_back, Result),

    {ok, _, [#{<<"count">> := <<"0">>}]} = pgc_client:execute(Connection, "select count(*) as count from pgc_client_test", []),

    ok = pgc_client:stop(Connection).

transaction_exception_rolls_back_test(Config) ->
    {ok, Connection} = pgc_client:start_link(connection_options(Config, #{})),

    {ok, _, []} = pgc_client:execute(Connection, "create table pgc_client_test (id int4)", []),

    ?assertError(boom, pgc_client:transaction(Connection, fun () ->
        {ok, _, []} = pgc_client:execute(Connection, "insert into pgc_client_test (id) values (1)", []),
        erlang:error(boom)
    end, #{})),

    {ok, _, [#{<<"count">> := <<"0">>}]} = pgc_client:execute(Connection, "select count(*) as count from pgc_client_test", []),
    ?assert(is_process_alive(Connection)),

    ok = pgc_client:stop(Connection).


% ------------------------------------------------------------------------------
% Helpers
% ------------------------------------------------------------------------------

connection_options(Config, Overrides) ->
    maps:merge(#{
        address => #{host => "127.0.0.1", port => ?config(postgres_port, Config)},
        tls => disable,
        user => ?POSTGRES_USER,
        password => ?POSTGRES_PASSWORD,
        database => ?POSTGRES_DATABASE,
        ping_interval => infinity
    }, Overrides).

start_postgres() ->
    % `exec:run/2`'s list-command form execs directly (no shell), so it doesn't
    % search $PATH like `os:cmd/1`'s shell-routed calls below do -- resolve it
    % ourselves.
    Docker = case os:find_executable("docker") of
        false -> ct:fail(docker_not_found);
        Path -> Path
    end,
    Name = "pgc-test-" ++ binary_to_list(binary:encode_hex(crypto:strong_rand_bytes(16), lowercase)),
    Cmd = [
        Docker,
        "run", "--rm",
        "--name", Name,
        "-e", "POSTGRES_USER=" ++ ?POSTGRES_USER,
        "-e", "POSTGRES_PASSWORD=" ++ ?POSTGRES_PASSWORD,
        "-e", "POSTGRES_DB=" ++ ?POSTGRES_DATABASE,
        "-p", "127.0.0.1::5432",
        ?POSTGRES_IMAGE
    ],
    % `exec:run_link/2` supervises this as a proper (foreground) OS child of
    % the `exec-port` program, not a detached background process, and links it to this
    % (the test case's own) process bidirectionally -- an unexpected container exit
    % fails this one test case, and this process exiting/crashing takes the container
    % down too.
    {ok, Pid, OsPid} = exec:run_link(Cmd, [stdout, stderr, monitor, {kill_timeout, 5}]),
    ok = wait_for_postgres_ready(OsPid),
    Port = postgres_port(Name),
    {Name, Pid, Port}.

-doc """
PostgreSQL's docker entrypoint logs "database system is ready to accept
connections" once for a temporary bootstrap instance (used to run init scripts,
reachable only over a local Unix socket, never the network) and again for the real
server -- waiting for it exactly twice, rather than polling `pg_isready` (which the
bootstrap instance also answers), avoids racing a connection attempt against that
first, throwaway one.
""".
wait_for_postgres_ready(OsPid) ->
    Deadline = pgc_deadline:from_timeout(60000),
    wait_for_postgres_ready(OsPid, <<>>, Deadline).

wait_for_postgres_ready(OsPid, Buffer, Deadline) ->
    case pgc_deadline:to_timeout(Deadline) of
        0 ->
            ct:fail(postgres_not_ready);
        Remaining ->
            receive
                {Stream, OsPid, Data} when Stream =:= stdout; Stream =:= stderr ->
                    NewBuffer = <<Buffer/binary, Data/binary>>,
                    Matches = binary:matches(NewBuffer, <<"database system is ready to accept connections">>),
                    case length(Matches) of
                        Count when Count >= 2 -> ok;
                        _ -> wait_for_postgres_ready(OsPid, NewBuffer, Deadline)
                    end
            after Remaining ->
                ct:fail(postgres_not_ready)
            end
    end.

postgres_port(Name) ->
    Output = string:trim(os:cmd("docker port " ++ Name ++ " 5432/tcp")),
    [Line | _] = string:split(Output, "\n"),
    [_Host, PortString] = string:split(Line, ":", trailing),
    erlang:list_to_integer(string:trim(PortString)).

stop_postgres(Pid) ->
    _ = exec:stop_and_wait(Pid, 10000),
    ok.
