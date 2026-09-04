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
    ping_keepalive_test/1,
    select_rows_test/1,
    multi_statement_query_test/1,
    invalid_statement_is_not_fatal_test/1,
    listen_notify_test/1,
    overlapping_query_casts_are_queued_test/1,
    invalid_handler_action_crashes_test/1,
    deferred_reply_test/1,
    prepare_and_execute_test/1,
    unprepare_test/1,
    prepare_error_is_not_fatal_test/1,
    chained_actions_from_one_callback_test/1
]).

-behaviour(pgc_connection).
-export([
    init/1,
    handle_ready/2,
    handle_notice/3,
    handle_notification/5,
    handle_row_data/4,
    handle_query_result/3,
    handle_prepare_result/3,
    handle_unprepare_result/3,
    handle_execute_result/3,
    handle_call/4,
    handle_cast/3,
    handle_info/3,
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
    % OTP 29's cross-module "native records" (`-export_record`/`-import_record`,
    % `#Mod:Record{}`) resolve the defining module's record shape at *runtime*, not
    % purely at compile time -- constructing or matching one before that module has
    % actually been loaded raises `badrecord`. A released node boots in embedded
    % mode, which preloads every module of every application up front, so this never
    % bites production code; a plain `erl` node (this test node included) loads code
    % lazily on first call instead, and record construction doesn't go through the
    % usual auto-load-on-undef path. Force it here rather than in `pgc`'s own
    % modules (e.g. via `-on_load`), since it's a test-node-only gap.
    {ok, Modules} = application:get_key(pgc, modules),
    lists:foreach(fun code:ensure_loaded/1, Modules),
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
            ping_keepalive_test,
            select_rows_test,
            multi_statement_query_test,
            invalid_statement_is_not_fatal_test,
            listen_notify_test,
            overlapping_query_casts_are_queued_test,
            invalid_handler_action_crashes_test,
            deferred_reply_test,
            prepare_and_execute_test,
            unprepare_test,
            prepare_error_is_not_fatal_test,
            chained_actions_from_one_callback_test
        ]}
    ].


% ------------------------------------------------------------------------------
% Test cases
% ------------------------------------------------------------------------------

connects_and_reaches_ready_test(Config) ->
    {ok, Connection} = pgc_connection:start_link(?MODULE, self(), connection_options(Config, #{})),
    ConnectionInfo = receive
        {handler, ready, Info} -> Info
    after 5000 ->
        ct:fail(no_ready_event)
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
            % 28P01) rather than failing SCRAM proof verification client-side --
            % `pgc_connection_statem_auth_sasl` surfaces that as `{auth_failure,
            % Fields}`, `Fields` being the raw `error_response_fields()` map.
            ?assertMatch({auth_failure, #{code := <<"28P01">>}}, Reason)
    after 5000 ->
        ct:fail(no_disconnected_event)
    end.

ping_keepalive_test(Config) ->
    {ok, Connection} = pgc_connection:start_link(?MODULE, self(), connection_options(Config, #{
        ping_interval => 200
    })),
    receive
        {handler, ready, _Info} -> ok
    after 5000 ->
        ct:fail(no_ready_event)
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
        {handler, ready, _Info} -> ok
    after 5000 ->
        ct:fail(no_ready_event)
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
        {handler, result, {ok, <<"SELECT 1">>}} -> ok
    after 5000 ->
        ct:fail(no_result)
    end,

    ok = pgc_connection:stop(Connection).

multi_statement_query_test(Config) ->
    {ok, Connection} = pgc_connection:start_link(?MODULE, self(), connection_options(Config, #{})),
    receive
        {handler, ready, _Info} -> ok
    after 5000 ->
        ct:fail(no_ready_event)
    end,

    ok = gen_statem:cast(Connection, {query, "select 1; select 2"}),
    receive {handler, row, _, [<<"1">>]} -> ok after 5000 -> ct:fail(no_row_1) end,
    receive {handler, result, {ok, <<"SELECT 1">>}} -> ok after 5000 -> ct:fail(no_result_1) end,
    receive {handler, row, _, [<<"2">>]} -> ok after 5000 -> ct:fail(no_row_2) end,
    receive {handler, result, {ok, <<"SELECT 1">>}} -> ok after 5000 -> ct:fail(no_result_2) end,

    % Back in #s_ready{} -- prove the connection is still usable.
    ok = gen_statem:cast(Connection, {query, "select 3"}),
    receive {handler, row, _, [<<"3">>]} -> ok after 5000 -> ct:fail(no_row_3) end,

    ok = pgc_connection:stop(Connection).

invalid_statement_is_not_fatal_test(Config) ->
    {ok, Connection} = pgc_connection:start_link(?MODULE, self(), connection_options(Config, #{})),
    receive
        {handler, ready, _Info} -> ok
    after 5000 ->
        ct:fail(no_ready_event)
    end,

    % There's no separate "error" callback -- a statement-level `ErrorResponse` reaches
    % the handler as `handle_query_result/3`'s `{error, Fields}`, the same callback a
    % successful `{ok, Tag}` goes through.
    ok = gen_statem:cast(Connection, {query, "select * from this_table_does_not_exist"}),
    receive
        {handler, result, {error, Error}} ->
            ?assertMatch(#{code := <<"42P01">>}, Error)
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
        {handler, ready, _} -> ok
    after 5000 ->
        ct:fail(no_ready_event)
    end,
    ok = gen_statem:cast(Listener, {query, "listen pgc_test_channel"}),
    receive
        {handler, result, {ok, <<"LISTEN">>}} -> ok
    after 5000 ->
        ct:fail(no_listen_result)
    end,

    {ok, Notifier} = pgc_connection:start_link(?MODULE, self(), connection_options(Config, #{})),
    receive
        {handler, ready, _} -> ok
    after 5000 ->
        ct:fail(no_ready_event)
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

overlapping_query_casts_are_queued_test(Config) ->
    {ok, Connection} = pgc_connection:start_link(?MODULE, self(), connection_options(Config, #{})),
    receive
        {handler, ready, _Info} -> ok
    after 5000 ->
        ct:fail(no_ready_event)
    end,

    % Both casts fire before the first statement's `ReadyForQuery` comes back --
    % `pgc_connection_statem_common`'s catch-all `internal, #simple_query{}` clause
    % answers `postpone` (the *gen_statem* action) for one that arrives outside
    % `#s_ready{}`, so the second query automatically waits its turn once the state
    % machine leaves `#s_simple_query{}`. The handler doesn't have to track phase or
    % postpone anything itself.
    ok = gen_statem:cast(Connection, {query, "select pg_sleep(0.2)"}),
    ok = gen_statem:cast(Connection, {query, "select 42 as answer"}),

    receive {handler, row, _, [<<>>]} -> ok after 5000 -> ct:fail(no_row_1) end,
    receive {handler, result, {ok, <<"SELECT 1">>}} -> ok after 5000 -> ct:fail(no_result_1) end,
    receive {handler, row, _, [<<"42">>]} -> ok after 5000 -> ct:fail(no_row_2) end,
    receive {handler, result, {ok, <<"SELECT 1">>}} -> ok after 5000 -> ct:fail(no_result_2) end,

    ok = pgc_connection:stop(Connection).

invalid_handler_action_crashes_test(Config) ->
    process_flag(trap_exit, true),
    {ok, Connection} = pgc_connection:start_link(?MODULE, self(), connection_options(Config, #{})),
    receive
        {handler, ready, _Info} -> ok
    after 5000 ->
        ct:fail(no_ready_event)
    end,

    % `boom` is this test's own deliberately-invalid trigger -- its `handle_call/4`
    % clause returns `bogus_action`, an action `pgc_connection_statem_common` doesn't
    % recognise (only `{query, _}` and `{reply, _, _}` are). There's no catch-all for
    % that; per the project's "let it crash" convention, the connection just crashes
    % instead of silently dropping the bad action. `gen_statem` passes that raw
    % `Reason` (not `{Reason, Stacktrace}`) to `terminate/2`.
    _ = spawn(fun () ->
        try gen_statem:call(Connection, boom, 2000) of
            _ -> ok
        catch
            _:_ -> ok
        end
    end),

    receive
        {handler, disconnected, {case_clause, bogus_action}} -> ok
    after 5000 ->
        ct:fail(connection_did_not_crash)
    end.

deferred_reply_test(Config) ->
    {ok, Connection} = pgc_connection:start_link(?MODULE, self(), connection_options(Config, #{})),
    receive
        {handler, ready, _Info} -> ok
    after 5000 ->
        ct:fail(no_ready_event)
    end,

    % `defer_reply` stashes `From` in the handler's own state instead of answering it --
    % the subsequent `NOTICE` is what actually triggers the `{reply, From, ok}` action,
    % from `handle_notice/3` rather than from the `handle_call/4` invocation that
    % received the call, proving `From` is a first-class, storable value.
    TestPid = self(),
    spawn(fun () -> TestPid ! {deferred_reply, gen_statem:call(Connection, defer_reply, infinity)} end),
    timer:sleep(100),
    ok = gen_statem:cast(Connection, {query, "do $$ begin raise notice 'ping'; end $$"}),

    receive
        {deferred_reply, ok} -> ok
    after 5000 ->
        ct:fail(no_deferred_reply)
    end,

    ok = pgc_connection:stop(Connection).

prepare_and_execute_test(Config) ->
    {ok, Connection} = pgc_connection:start_link(?MODULE, self(), connection_options(Config, #{})),
    receive
        {handler, ready, _Info} -> ok
    after 5000 ->
        ct:fail(no_ready_event)
    end,

    ok = gen_statem:cast(Connection, {prepare, ~"s1", "select $1::int4 as n"}),
    receive
        {handler, prepared, {ok, ~"s1", Description}} ->
            ?assertMatch(
                #{parameters_description := [_], row_description := [#pgc_protocol_message:row_description_field{name = <<"n">>}]},
                Description
            )
    after 5000 ->
        ct:fail(no_prepared)
    end,

    ok = gen_statem:cast(Connection, {execute, ~"s1", [{text, <<"42">>}], #{}}),
    receive {handler, row, _Fields, [<<"42">>]} -> ok after 5000 -> ct:fail(no_row) end,
    receive {handler, executed, {ok, <<"SELECT 1">>}} -> ok after 5000 -> ct:fail(no_executed) end,

    ok = pgc_connection:stop(Connection).

unprepare_test(Config) ->
    {ok, Connection} = pgc_connection:start_link(?MODULE, self(), connection_options(Config, #{})),
    receive
        {handler, ready, _Info} -> ok
    after 5000 ->
        ct:fail(no_ready_event)
    end,

    ok = gen_statem:cast(Connection, {prepare, ~"s1", "select 1 as n"}),
    receive {handler, prepared, {ok, ~"s1", _}} -> ok after 5000 -> ct:fail(no_prepared) end,

    ok = gen_statem:cast(Connection, {execute, ~"s1", [], #{}}),
    receive {handler, row, _, [<<"1">>]} -> ok after 5000 -> ct:fail(no_row) end,
    receive {handler, executed, {ok, _}} -> ok after 5000 -> ct:fail(no_executed) end,

    ok = gen_statem:cast(Connection, {unprepare, ~"s1"}),
    receive {handler, unprepared, {ok, ~"s1"}} -> ok after 5000 -> ct:fail(no_unprepared) end,

    % The statement is gone -- executing it again fails at Bind time (invalid_sql_statement_name),
    % but the connection itself stays healthy.
    ok = gen_statem:cast(Connection, {execute, ~"s1", [], #{}}),
    receive
        {handler, executed, {error, Error}} ->
            ?assertMatch(#{code := <<"26000">>}, Error)
    after 5000 ->
        ct:fail(no_error)
    end,
    ?assert(is_process_alive(Connection)),

    ok = pgc_connection:stop(Connection).

prepare_error_is_not_fatal_test(Config) ->
    {ok, Connection} = pgc_connection:start_link(?MODULE, self(), connection_options(Config, #{})),
    receive
        {handler, ready, _Info} -> ok
    after 5000 ->
        ct:fail(no_ready_event)
    end,

    % Same "no separate error callback" shape as `handle_query_result/3` -- a failed
    % `Parse` reaches the handler as `handle_prepare_result/3`'s `{error, Fields}`.
    ok = gen_statem:cast(Connection, {prepare, ~"bad", "not valid sql"}),
    receive
        {handler, prepared, {error, Error}} ->
            ?assertMatch(#{code := <<"42601">>}, Error)
    after 5000 ->
        ct:fail(no_error)
    end,
    ?assert(is_process_alive(Connection)),

    % Not fatal -- the connection is still usable afterward.
    ok = gen_statem:cast(Connection, {query, "select 1"}),
    receive {handler, row, _, [<<"1">>]} -> ok after 5000 -> ct:fail(no_row) end,

    ok = pgc_connection:stop(Connection).

chained_actions_from_one_callback_test(Config) ->
    {ok, Connection} = pgc_connection:start_link(?MODULE, self(), connection_options(Config, #{})),
    receive
        {handler, ready, _Info} -> ok
    after 5000 ->
        ct:fail(no_ready_event)
    end,

    ok = gen_statem:cast(Connection, {prepare, ~"s1", "select 1 as n"}),
    receive {handler, prepared, {ok, ~"s1", _}} -> ok after 5000 -> ct:fail(no_prepared) end,

    % One handler return chains three primitives -- unprepare, re-prepare under the
    % same name with a different statement, execute the new definition -- with no
    % batching machinery: each is postponed in turn, in order, until the state machine
    % is back in #s_ready{} for it.
    ok = gen_statem:cast(Connection, {reprepare, ~"s1", "select 2 as n"}),

    receive {handler, unprepared, {ok, ~"s1"}} -> ok after 5000 -> ct:fail(no_unprepared) end,
    receive {handler, prepared, {ok, ~"s1", _}} -> ok after 5000 -> ct:fail(no_prepared_2) end,
    receive {handler, row, _, [<<"2">>]} -> ok after 5000 -> ct:fail(no_row) end,
    receive {handler, executed, {ok, _}} -> ok after 5000 -> ct:fail(no_executed) end,

    ok = pgc_connection:stop(Connection).


% ------------------------------------------------------------------------------
% pgc_connection handler callbacks
% ------------------------------------------------------------------------------

-doc false.
init(TestPid) ->
    {ok, {TestPid, undefined}}.

-doc false.
handle_ready(ConnectionInfo, {TestPid, Pending}) ->
    TestPid ! {handler, ready, ConnectionInfo},
    {[], {TestPid, Pending}}.

-doc false.
terminate(Reason, {TestPid, _Pending}) ->
    TestPid ! {handler, disconnected, Reason},
    ok.

-doc false.
handle_notice(_ConnectionInfo, _Fields, {TestPid, undefined}) ->
    {[], {TestPid, undefined}};
handle_notice(_ConnectionInfo, Fields, {TestPid, From}) ->
    % `deferred_reply_test`'s own trigger -- answers a `From` stashed by an earlier
    % `defer_reply` call, from this unrelated callback invocation instead.
    TestPid ! {handler, notice, Fields},
    {[{reply, From, ok}], {TestPid, undefined}}.

-doc false.
handle_notification(_ConnectionInfo, SenderId, Channel, Payload, {TestPid, Pending}) ->
    TestPid ! {handler, notification, Channel, Payload, SenderId},
    {[], {TestPid, Pending}}.

-doc false.
handle_row_data(_ConnectionInfo, RowDescription, Row, {TestPid, Pending}) ->
    TestPid ! {handler, row, RowDescription, Row},
    {[], {TestPid, Pending}}.

-doc false.
handle_query_result(_ConnectionInfo, Result, {TestPid, Pending}) ->
    % `Result` is `empty | {ok, Tag :: binary()}` for a completed statement, or
    % `{error, Fields}` for one that failed -- there's no separate error callback.
    TestPid ! {handler, result, Result},
    {[], {TestPid, Pending}}.

-doc false.
handle_prepare_result(_ConnectionInfo, Result, {TestPid, Pending}) ->
    TestPid ! {handler, prepared, Result},
    {[], {TestPid, Pending}}.

-doc false.
handle_unprepare_result(_ConnectionInfo, Result, {TestPid, Pending}) ->
    TestPid ! {handler, unprepared, Result},
    {[], {TestPid, Pending}}.

-doc false.
handle_execute_result(_ConnectionInfo, Result, {TestPid, Pending}) ->
    TestPid ! {handler, executed, Result},
    {[], {TestPid, Pending}}.

-doc false.
handle_call(_ConnectionInfo, defer_reply, From, {TestPid, _Pending}) ->
    {[], {TestPid, From}};
handle_call(_ConnectionInfo, boom, _From, {TestPid, Pending}) ->
    % `invalid_handler_action_crashes_test`'s own trigger -- `bogus_action` isn't a
    % recognised `pgc_connection:action()`, so returning it crashes the connection.
    {[bogus_action], {TestPid, Pending}};
handle_call(_ConnectionInfo, Request, _From, {TestPid, Pending}) ->
    TestPid ! {handler, call, Request},
    {[], {TestPid, Pending}}.

-doc false.
handle_cast(_ConnectionInfo, {query, Sql}, {TestPid, Pending}) ->
    % No need to check phase/readiness first -- a `{query, _}` action that arrives
    % while a statement is already in flight is postponed automatically by
    % `pgc_connection_statem_common` until the connection is back in `#s_ready{}`.
    {[{query, Sql}], {TestPid, Pending}};
handle_cast(_ConnectionInfo, {prepare, Name, Text}, {TestPid, Pending}) ->
    {[{prepare, Name, Text}], {TestPid, Pending}};
handle_cast(_ConnectionInfo, {unprepare, Name}, {TestPid, Pending}) ->
    {[{unprepare, Name}], {TestPid, Pending}};
handle_cast(_ConnectionInfo, {execute, Name, Parameters, Options}, {TestPid, Pending}) ->
    {[{execute, Name, Parameters, Options}], {TestPid, Pending}};
handle_cast(_ConnectionInfo, {reprepare, Name, NewText}, {TestPid, Pending}) ->
    % `chained_actions_from_one_callback_test`'s own trigger -- one callback return
    % batches three primitives, chained purely via the ordinary postpone-until-ready
    % mechanism, no extra wiring needed on either side.
    {[{unprepare, Name}, {prepare, Name, NewText}, {execute, Name, [], #{}}], {TestPid, Pending}}.

-doc false.
handle_info(_ConnectionInfo, _Info, {TestPid, Pending}) ->
    {[], {TestPid, Pending}}.


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
