-module(pgc_connection_SUITE).
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
    connects_and_reaches_ready_test/1,
    wrong_password_stops_with_error_test/1,
    owner_down_stops_cleanly_test/1,
    ping_keepalive_test/1,
    select_rows_test/1,
    multi_statement_query_test/1,
    invalid_statement_is_not_fatal_test/1,
    listen_notify_test/1,
    call_while_busy_crashes_test/1
]).

-behaviour(pgc_connection).
-export([
    init/1,
    handle_connected/2,
    handle_notice/2,
    handle_notification/4,
    handle_row_data/3,
    handle_result/2,
    handle_error/2,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2
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
    Config.

-doc false.
end_per_suite(_Config) ->
    ok = application:stop(pgc),
    ok.

-doc """
Spawns a throwaway PostgreSQL container per test case (via `erlexec`, so it's a
properly supervised OS child -- reliably cleaned up even if this node dies uncleanly,
unlike a `docker run -d` fire-and-forget background container), per the project's
convention of not depending on any pre-provisioned server. Cases run in parallel (see
`groups/0`), each against its own container, to keep the container-per-case isolation
without paying for it sequentially.

`init_per_testcase/2`, the test case itself, and `end_per_testcase/2` run in the same
process for a given case, so `exec:run_link/2` here correctly ties this container's
lifetime to *this test case's* process -- unlike linking from `init_per_suite/1`, whose
process doesn't survive to protect anything for the rest of the suite.
""".
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
            connects_and_reaches_ready_test,
            wrong_password_stops_with_error_test,
            owner_down_stops_cleanly_test,
            ping_keepalive_test,
            select_rows_test,
            multi_statement_query_test,
            invalid_statement_is_not_fatal_test,
            listen_notify_test,
            call_while_busy_crashes_test
        ]}
    ].


% ------------------------------------------------------------------------------
% Test cases
% ------------------------------------------------------------------------------

connects_and_reaches_ready_test(Config) ->
    {ok, Connection} = pgc_connection:start_link(?MODULE, self(), connection_options(Config, #{})),
    ConnectionInfo = receive
        {handler, connected, Info} -> Info
    after 5000 ->
        ct:fail(no_connected_event)
    end,
    % `backend_key` isn't currently surfaced in connection_info() (WIP).
    #{parameters := Parameters} = ConnectionInfo,
    ?assertMatch(#{<<"server_version">> := _}, Parameters),
    ?assert(is_process_alive(Connection)),

    ok = pgc_connection:stop(Connection),
    receive
        {handler, disconnected, normal} -> ok
    after 5000 ->
        ct:fail(no_disconnected_event)
    end.

wrong_password_stops_with_error_test(Config) ->
    {ok, _Connection} = pgc_connection:start_link(?MODULE, self(), connection_options(Config, #{
        password => "definitely-the-wrong-password"
    })),
    receive
        {handler, disconnected, Reason} ->
            % Real PostgreSQL rejects a wrong password itself (`ErrorResponse`,
            % 28P01) rather than failing SCRAM proof verification client-side.
            ?assertMatch(#pgc_protocol:error{code = <<"28P01">>}, Reason)
    after 5000 ->
        ct:fail(no_disconnected_event)
    end.

owner_down_stops_cleanly_test(Config) ->
    {Owner, OwnerMonitor} = spawn_monitor(fun () ->
        receive stop -> ok end
    end),
    {ok, Connection} = pgc_connection:start_link(?MODULE, self(), connection_options(Config, #{}), Owner),
    receive
        {handler, connected, _Info} -> ok
    after 5000 ->
        ct:fail(no_connected_event)
    end,

    ConnectionMonitor = erlang:monitor(process, Connection),
    Owner ! stop,
    receive {'DOWN', OwnerMonitor, process, Owner, _} -> ok end,

    receive
        {handler, disconnected, normal} -> ok
    after 5000 ->
        ct:fail(no_disconnected_event)
    end,
    receive
        {'DOWN', ConnectionMonitor, process, Connection, _} -> ok
    after 5000 ->
        ct:fail(connection_did_not_stop)
    end,
    ok.

ping_keepalive_test(Config) ->
    {ok, Connection} = pgc_connection:start_link(?MODULE, self(), connection_options(Config, #{
        ping_interval => 200
    })),
    receive
        {handler, connected, _Info} -> ok
    after 5000 ->
        ct:fail(no_connected_event)
    end,

    % Several ping/keepalive round-trips (send Sync, expect ReadyForQuery) should
    % happen without the connection ever considering itself dead.
    timer:sleep(1200),
    ?assert(is_process_alive(Connection)),

    ok = pgc_connection:stop(Connection),
    receive
        {handler, disconnected, normal} -> ok
    after 5000 ->
        ct:fail(no_disconnected_event)
    end.

select_rows_test(Config) ->
    {ok, Connection} = pgc_connection:start_link(?MODULE, self(), connection_options(Config, #{})),
    receive
        {handler, connected, _Info} -> ok
    after 5000 ->
        ct:fail(no_connected_event)
    end,

    ok = gen_statem:cast(Connection, {query, "select 42 as answer, 'hi' as greeting"}),
    receive
        {handler, row, Fields, Values} ->
            ?assertMatch(
                [#pgc_protocol_message:row_description_field{name = <<"answer">>},
                 #pgc_protocol_message:row_description_field{name = <<"greeting">>}],
                Fields
            ),
            ?assertEqual([<<"42">>, <<"hi">>], Values)
    after 5000 ->
        ct:fail(no_row)
    end,
    receive
        {handler, result, <<"SELECT 1">>} -> ok
    after 5000 ->
        ct:fail(no_result)
    end,

    ok = pgc_connection:stop(Connection).

multi_statement_query_test(Config) ->
    {ok, Connection} = pgc_connection:start_link(?MODULE, self(), connection_options(Config, #{})),
    receive
        {handler, connected, _Info} -> ok
    after 5000 ->
        ct:fail(no_connected_event)
    end,

    ok = gen_statem:cast(Connection, {query, "select 1; select 2"}),
    receive {handler, row, _, [<<"1">>]} -> ok after 5000 -> ct:fail(no_row_1) end,
    receive {handler, result, <<"SELECT 1">>} -> ok after 5000 -> ct:fail(no_result_1) end,
    receive {handler, row, _, [<<"2">>]} -> ok after 5000 -> ct:fail(no_row_2) end,
    receive {handler, result, <<"SELECT 1">>} -> ok after 5000 -> ct:fail(no_result_2) end,

    % Back in #ready{} -- prove the connection is still usable.
    ok = gen_statem:cast(Connection, {query, "select 3"}),
    receive {handler, row, _, [<<"3">>]} -> ok after 5000 -> ct:fail(no_row_3) end,

    ok = pgc_connection:stop(Connection).

invalid_statement_is_not_fatal_test(Config) ->
    {ok, Connection} = pgc_connection:start_link(?MODULE, self(), connection_options(Config, #{})),
    receive
        {handler, connected, _Info} -> ok
    after 5000 ->
        ct:fail(no_connected_event)
    end,

    ok = gen_statem:cast(Connection, {query, "select * from this_table_does_not_exist"}),
    receive
        {handler, error, Error} ->
            ?assertMatch(#pgc_protocol:error{code = <<"42P01">>}, Error)
    after 5000 ->
        ct:fail(no_error)
    end,
    ?assert(is_process_alive(Connection)),

    % Not fatal -- the connection is still usable afterward.
    ok = gen_statem:cast(Connection, {query, "select 1"}),
    receive {handler, row, _, [<<"1">>]} -> ok after 5000 -> ct:fail(no_row) end,

    ok = pgc_connection:stop(Connection).

listen_notify_test(Config) ->
    {ok, Listener} = pgc_connection:start_link(?MODULE, self(), connection_options(Config, #{})),
    receive
        {handler, connected, _} -> ok
    after 5000 ->
        ct:fail(no_connected_event)
    end,
    ok = gen_statem:cast(Listener, {query, "listen pgc_test_channel"}),
    receive
        {handler, result, <<"LISTEN">>} -> ok
    after 5000 ->
        ct:fail(no_listen_result)
    end,

    {ok, Notifier} = pgc_connection:start_link(?MODULE, self(), connection_options(Config, #{})),
    receive
        {handler, connected, _} -> ok
    after 5000 ->
        ct:fail(no_connected_event)
    end,
    ok = gen_statem:cast(Notifier, {query, "notify pgc_test_channel, 'hello'"}),

    receive
        {handler, notification, <<"pgc_test_channel">>, <<"hello">>, SenderId} ->
            ?assert(is_integer(SenderId))
    after 5000 ->
        ct:fail(no_notification)
    end,

    ok = pgc_connection:stop(Listener),
    ok = pgc_connection:stop(Notifier).

call_while_busy_crashes_test(Config) ->
    process_flag(trap_exit, true),
    {ok, Connection} = pgc_connection:start_link(?MODULE, self(), connection_options(Config, #{})),
    receive
        {handler, connected, _Info} -> ok
    after 5000 ->
        ct:fail(no_connected_event)
    end,

    % A cast arriving while a query is already in flight has no matching clause in the
    % sub-protocol module and falls through to `pgc_connection:handle_common_event/4`,
    % which doesn't handle `cast` either -- `function_clause`, by design (see the plan's
    % "Open items": nothing currently postpones it). `gen_statem` passes that raw
    % `Reason` (not `{Reason, Stacktrace}`) to `terminate/3`.
    ok = gen_statem:cast(Connection, {query, "select pg_sleep(0.5)"}),
    ok = gen_statem:cast(Connection, {query, "select 1"}),

    receive
        {handler, disconnected, function_clause} -> ok
    after 5000 ->
        ct:fail(connection_did_not_crash)
    end.


% ------------------------------------------------------------------------------
% pgc_connection handler callbacks
% ------------------------------------------------------------------------------

-doc false.
init(TestPid) ->
    {ok, TestPid}.

-doc false.
handle_connected(ConnectionInfo, TestPid) ->
    TestPid ! {handler, connected, ConnectionInfo},
    {ok, TestPid}.

-doc false.
terminate(Reason, TestPid) ->
    TestPid ! {handler, disconnected, Reason},
    ok.

-doc false.
handle_notice(Fields, TestPid) ->
    TestPid ! {handler, notice, Fields},
    {ok, TestPid}.

-doc false.
handle_notification(Channel, Payload, SenderId, TestPid) ->
    TestPid ! {handler, notification, Channel, Payload, SenderId},
    {ok, TestPid}.

-doc false.
handle_row_data(RowDescription, Row, TestPid) ->
    TestPid ! {handler, row, RowDescription, Row},
    {ok, TestPid}.

-doc false.
handle_result(CommandTag, TestPid) ->
    TestPid ! {handler, result, CommandTag},
    {ok, TestPid}.

-doc false.
handle_error(Error, TestPid) ->
    TestPid ! {handler, error, Error},
    {ok, TestPid}.

-doc false.
handle_call(Request, _From, TestPid) ->
    TestPid ! {handler, call, Request},
    {noreply, TestPid}.

-doc false.
handle_cast({query, Sql}, TestPid) ->
    {query, Sql, TestPid}.

-doc false.
handle_info(_Info, TestPid) ->
    {noreply, TestPid}.


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
