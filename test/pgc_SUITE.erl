-module(pgc_SUITE).
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

% Used by child_spec_3_test/child_spec_4_test as a throwaway supervisor to embed a
% `pgc:child_spec/3,4` under, the way a real application would.
-behaviour(supervisor).
-export([
    init/1
]).

-export([
    start_link_2_execute_stop_test/1,
    start_link_3_named_test/1,
    child_spec_3_test/1,
    child_spec_4_test/1,
    execute_3_row_option_test/1,
    transaction_commit_test/1,
    transaction_rollback_test/1,
    transaction_nested_error_test/1,
    execute_stale_transaction_ref_test/1
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
            start_link_2_execute_stop_test,
            start_link_3_named_test,
            child_spec_3_test,
            child_spec_4_test,
            execute_3_row_option_test,
            transaction_commit_test,
            transaction_rollback_test,
            transaction_nested_error_test,
            execute_stale_transaction_ref_test
        ]}
    ].


% ------------------------------------------------------------------------------
% Test cases
% ------------------------------------------------------------------------------

start_link_2_execute_stop_test(Config) ->
    {ok, Pool} = pgc:start_link(client_options(Config, #{}), #{max_size => 1}),
    ?assertMatch({ok, #{}, [#{<<"n">> := 1}]}, pgc:execute(Pool, "select 1 as n")),
    ?assertEqual(ok, pgc:stop(Pool)).

start_link_3_named_test(Config) ->
    Name = list_to_atom("pgc_SUITE_pool_" ++ integer_to_list(erlang:unique_integer([positive]))),
    {ok, _Pool} = pgc:start_link({local, Name}, client_options(Config, #{}), #{max_size => 1}),
    ?assertMatch({ok, #{}, [#{<<"n">> := 1}]}, pgc:execute(Name, "select 1 as n")),
    ?assertEqual(ok, pgc:stop(Name)).

child_spec_3_test(Config) ->
    ChildSpec = pgc:child_spec(pool, client_options(Config, #{}), #{max_size => 1}),
    {ok, Sup} = supervisor:start_link(?MODULE, [ChildSpec]),
    {ok, {pool, Pool, supervisor, _Modules}} = supervisor:which_child(Sup, pool),
    ?assertMatch({ok, #{}, [#{<<"n">> := 1}]}, pgc:execute(Pool, "select 1 as n")).

child_spec_4_test(Config) ->
    Name = list_to_atom("pgc_SUITE_named_pool_" ++ integer_to_list(erlang:unique_integer([positive]))),
    ChildSpec = pgc:child_spec(pool, {local, Name}, client_options(Config, #{}), #{max_size => 1}),
    {ok, _Sup} = supervisor:start_link(?MODULE, [ChildSpec]),
    ?assertMatch({ok, #{}, [#{<<"n">> := 1}]}, pgc:execute(Name, "select 1 as n")).

-doc false.
init(ChildSpecs) ->
    {ok, {#{strategy => one_for_one}, ChildSpecs}}.

execute_3_row_option_test(Config) ->
    {ok, Pool} = pgc:start_link(client_options(Config, #{}), #{max_size => 1}),
    ?assertMatch({ok, #{}, [[1]]}, pgc:execute(Pool, "select 1 as n", #{row => list})).

transaction_commit_test(Config) ->
    {ok, Pool} = pgc:start_link(client_options(Config, #{}), #{max_size => 1}),
    {ok, _, []} = pgc:execute(Pool, "create table committed (n integer)"),
    Result = pgc:transaction(Pool, fun (Tx) ->
        {ok, _, []} = pgc:execute(Tx, "insert into committed values (1)"),
        {commit, done}
    end),
    ?assertEqual(done, Result),
    ?assertMatch({ok, #{}, [#{<<"n">> := 1}]}, pgc:execute(Pool, "select n from committed")).

transaction_rollback_test(Config) ->
    {ok, Pool} = pgc:start_link(client_options(Config, #{}), #{max_size => 1}),
    {ok, _, []} = pgc:execute(Pool, "create table rolled_back (n integer)"),
    Result = pgc:transaction(Pool, fun (Tx) ->
        {ok, _, []} = pgc:execute(Tx, "insert into rolled_back values (1)"),
        {rollback, done}
    end),
    ?assertEqual(done, Result),
    ?assertMatch({ok, #{}, []}, pgc:execute(Pool, "select n from rolled_back")).

transaction_nested_error_test(Config) ->
    {ok, Pool} = pgc:start_link(client_options(Config, #{}), #{max_size => 1}),
    ?assertError(in_transaction, pgc:transaction(Pool, fun (_Tx) ->
        {commit, pgc:transaction(Pool, fun (_) -> {commit, unreachable} end)}
    end)).

execute_stale_transaction_ref_test(Config) ->
    {ok, Pool} = pgc:start_link(client_options(Config, #{}), #{max_size => 1}),
    StaleRef = pgc:transaction(Pool, fun (Tx) -> {commit, Tx} end),
    ?assertError(not_in_transaction, pgc:execute(StaleRef, "select 1")).


% ------------------------------------------------------------------------------
% Helpers
% ------------------------------------------------------------------------------

client_options(Config, Overrides) ->
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
