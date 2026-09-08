-module(pgc_pool_SUITE).
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
    checkout_runs_action_and_returns_connection_test/1,
    checkout_waits_for_available_connection_test/1,
    checkout_timeout_does_not_leak_connection_test/1,
    checked_out_caller_death_frees_connection_test/1,
    checkin_resets_transaction_and_session_state_test/1
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
            checkout_runs_action_and_returns_connection_test,
            checkout_waits_for_available_connection_test,
            checkout_timeout_does_not_leak_connection_test,
            checked_out_caller_death_frees_connection_test,
            checkin_resets_transaction_and_session_state_test
        ]}
    ].


% ------------------------------------------------------------------------------
% Test cases
% ------------------------------------------------------------------------------

% None of these call `pgc_pool:stop/1`: `pgc_pool:start_link/2` links the pool supervisor to its
% caller (this test process), same as any `supervisor:start_link/2,3`; `stop/1`'s `gen_server:stop`
% on the manager makes the whole tree auto-shut-down with reason `shutdown`, which -- since this
% test process isn't itself trapping exits, unlike the real supervisor `pgc_pool:start_link/2` is
% meant to be started under -- would kill it too via that same link. The pool is left to go away on
% its own once the test case process exits.

checkout_runs_action_and_returns_connection_test(Config) ->
    {ok, Pool} = pgc_pool:start_link(client_options(Config, #{}), #{max_size => 1}),
    ?assertMatch(#{available := 0, used := 0, size := 0, max_size := 1}, pgc_pool:info(Pool)),
    Result = pgc_pool:with_client(Pool, fun (Connection) ->
        ?assertMatch(#{available := 0, used := 1, size := 1, max_size := 1}, pgc_pool:info(Pool)),
        pgc_client:execute(Connection, "select 'hello Erlang' as message", [])
    end),
    ?assertMatch({ok, #{}, [#{<<"message">> := <<"hello Erlang">>}]}, Result),
    ?assertMatch(#{available := 1, used := 0, size := 1, max_size := 1}, pgc_pool:info(Pool)).

-define(wait(Message, Timeout), receive Message -> ok after Timeout -> ?assert(false, "Timed out") end).
checkout_waits_for_available_connection_test(Config) ->
    {ok, Pool} = pgc_pool:start_link(client_options(Config, #{}), #{max_size => 1}),
    Parent = self(),
    User1 = spawn_link(fun () ->
        Parent ! {self(), result, pgc_pool:with_client(Pool, fun (Connection) ->
            Parent ! {self(), got_connection},
            {ok, #{}, [#{<<"message">> := Message}]} = pgc_client:execute(Connection, "select 'from User1' as message", []),
            ?wait(continue, 1000),
            Message
        end)}
    end),
    receive {User1, got_connection} -> ok end,
    User2 = spawn_link(fun () ->
        Parent ! {self(), waiting_for_connection},
        Parent ! {self(), result, pgc_pool:with_client(Pool, fun (Connection) ->
            Parent ! {self(), got_connection},
            {ok, #{}, [#{<<"message">> := Message}]} = pgc_client:execute(Connection, "select 'from User2' as message", []),
            Message
        end)}
    end),
    receive {User2, waiting_for_connection} -> ok end,
    receive
        {User2, got_connection} -> ct:fail("Should not be reached before User1 checks in")
    after 200 ->
        ?assertMatch(#{available := 0, used := 1, size := 1, max_size := 1}, pgc_pool:info(Pool))
    end,
    User1 ! continue,
    receive {User1, result, User1Message} -> ?assertEqual(<<"from User1">>, User1Message) end,
    receive {User2, got_connection} -> ok end,
    receive {User2, result, User2Message} -> ?assertEqual(<<"from User2">>, User2Message) end.

checkout_timeout_does_not_leak_connection_test(Config) ->
    {ok, Pool} = pgc_pool:start_link(client_options(Config, #{}), #{max_size => 1}),
    Parent = self(),
    Holder = spawn_link(fun () ->
        pgc_pool:with_client(Pool, fun (_Connection) ->
            Parent ! holding,
            ?wait(release, 1000)
        end)
    end),
    receive holding -> ok end,
    ?assertError({pgc, pool_timeout}, pgc_pool:with_client(Pool, fun (_) -> ct:fail(unreachable) end, #{timeout => 100})),
    Holder ! release,
    % The timed-out checkout must not have been silently granted once the connection freed up --
    % it should still be available for a brand new checkout instead of stuck forever in `used`.
    Result = pgc_pool:with_client(Pool, fun (Connection) ->
        pgc_client:execute(Connection, "select 1 as n", [])
    end, #{timeout => 1000}),
    ?assertMatch({ok, #{}, [#{<<"n">> := 1}]}, Result).

checked_out_caller_death_frees_connection_test(Config) ->
    {ok, Pool} = pgc_pool:start_link(client_options(Config, #{}), #{max_size => 1}),
    Parent = self(),
    Killed = spawn(fun () ->
        pgc_pool:with_client(Pool, fun (_Connection) ->
            Parent ! ready,
            ?wait(never_sent, 1000)
        end)
    end),
    receive ready -> ok end,
    exit(Killed, kill),
    Result = pgc_pool:with_client(Pool, fun (Connection) ->
        pgc_client:execute(Connection, "select 1 as n", [])
    end, #{timeout => 1000}),
    ?assertMatch({ok, #{}, [#{<<"n">> := 1}]}, Result).

checkin_resets_transaction_and_session_state_test(Config) ->
    {ok, Pool} = pgc_pool:start_link(client_options(Config, #{}), #{max_size => 1}),
    % Leaves both a session-level `set` and an open (never committed/rolled back) transaction
    % behind -- `with_client/3`'s checkin-time `pgc_client:reset/1` must clean up both before the
    % next checkout, or this GUC (and the open transaction) would otherwise leak across callers.
    pgc_pool:with_client(Pool, fun (Connection) ->
        {ok, _, []} = pgc_client:execute(Connection, "set statement_timeout = '1s'", []),
        {ok, _, []} = pgc_client:execute(Connection, "start transaction", [])
    end),
    Result = pgc_pool:with_client(Pool, fun (Connection) ->
        {ok, _, [#{<<"timeout">> := Timeout}]} = pgc_client:execute(Connection,
            "select current_setting('statement_timeout') as timeout", []),
        Timeout
    end),
    ?assertEqual(<<"0">>, Result).


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
