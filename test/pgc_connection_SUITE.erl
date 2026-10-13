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
    idle_transaction_timeout_stops_with_error_test/1,
    query_during_ping_gets_its_own_result_test/1,
    cancel_is_retried_until_it_takes_effect_test/1,
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
    handle_row_data/5,
    handle_query_result/4,
    handle_prepare_result/4,
    handle_unprepare_result/4,
    handle_execute_result/4,
    handle_call/4,
    handle_cast/3,
    handle_info/3,
    terminate/2
]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").


-doc false.
suite() ->
    [
        {timetrap, {seconds, 60}},
        {require, postgresql_server_host},
        {require, postgresql_server_port},
        {require, postgresql_server_user},
        {require, postgresql_server_password},
        {require, postgresql_server_database}
    ].

-doc false.
init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(pgc),
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

-doc false.
init_per_testcase(_Case, Config) ->
    [{database, create_database()} | Config].

-doc false.
end_per_testcase(_Case, Config) ->
    drop_database(?config(database, Config)).

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
            idle_transaction_timeout_stops_with_error_test,
            query_during_ping_gets_its_own_result_test,
            cancel_is_retried_until_it_takes_effect_test,
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

idle_transaction_timeout_stops_with_error_test(Config) ->
    {ok, Connection} = pgc_connection:start_link(?MODULE, self(), connection_options(Config, #{})),
    receive
        {handler, ready, _Info} -> ok
    after 5000 ->
        ct:fail(no_ready_event)
    end,

    ok = gen_statem:cast(Connection, {query, make_ref(), "set idle_in_transaction_session_timeout = 100"}),
    receive {handler, result, {ok, <<"SET">>}} -> ok after 5000 -> ct:fail(no_set_result) end,
    ok = gen_statem:cast(Connection, {query, make_ref(), "begin"}),
    receive {handler, result, {ok, <<"BEGIN">>}} -> ok after 5000 -> ct:fail(no_begin_result) end,

    receive
        {handler, disconnected, Reason} ->
            ?assertMatch({error_response, #{code := <<"25P03">>}}, Reason)
    after 5000 ->
        ct:fail(no_disconnected_event)
    end.

query_during_ping_gets_its_own_result_test(_Config) ->
    {Backend, Port} = start_backend(),
    {ok, Connection} = pgc_connection:start_link(?MODULE, self(), #{
        address => #{host => "127.0.0.1", port => Port},
        tls => disable,
        user => "pgc",
        database => "pgc",
        ping_interval => 500
    }),
    receive {handler, ready, _Info} -> ok after 5000 -> ct:fail(no_ready_event) end,
    receive {backend, notice_sent} -> ok after 5000 -> ct:fail(no_ping) end,
    receive {handler, notice, _Fields} -> ok after 5000 -> ct:fail(no_notice) end,

    ok = gen_statem:cast(Connection, {query, make_ref(), "select 1 as n"}),
    timer:sleep(100),
    Backend ! {backend, ready_for_query},

    receive {handler, row, _RowDescription, Values} -> ?assertEqual([<<"1">>], Values) after 5000 -> ct:fail(no_row) end,
    receive {handler, result, Result} -> ?assertEqual({ok, <<"SELECT 1">>}, Result) after 5000 -> ct:fail(no_result) end,

    ok = pgc_connection:stop(Connection).

cancel_is_retried_until_it_takes_effect_test(_Config) ->
    Port = start_cancelling_backend(),
    {ok, Connection} = pgc_connection:start_link(?MODULE, self(), #{
        address => #{host => "127.0.0.1", port => Port},
        tls => disable,
        user => "pgc",
        database => "pgc",
        ping_interval => infinity
    }),
    receive {handler, ready, _Info} -> ok after 5000 -> ct:fail(no_ready_event) end,

    Ref = make_ref(),
    ok = gen_statem:cast(Connection, {query, Ref, "select pg_sleep(10)"}),
    ok = gen_statem:cast(Connection, {cancel, Ref}),
    receive {backend, cancel_ignored} -> ok after 5000 -> ct:fail(no_cancel_request) end,
    receive {handler, result, Result} -> ?assertMatch({error, #{code := <<"57014">>}}, Result) after 5000 -> ct:fail(no_result) end,

    ok = pgc_connection:stop(Connection).

select_rows_test(Config) ->
    {ok, Connection} = pgc_connection:start_link(?MODULE, self(), connection_options(Config, #{})),
    receive
        {handler, ready, _Info} -> ok
    after 5000 ->
        ct:fail(no_ready_event)
    end,

    ok = gen_statem:cast(Connection, {query, make_ref(), "select 42 as answer, 'hi' as greeting"}),
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

    ok = gen_statem:cast(Connection, {query, make_ref(), "select 1; select 2"}),
    receive {handler, row, _, [<<"1">>]} -> ok after 5000 -> ct:fail(no_row_1) end,
    receive {handler, result, {ok, <<"SELECT 1">>}} -> ok after 5000 -> ct:fail(no_result_1) end,
    receive {handler, row, _, [<<"2">>]} -> ok after 5000 -> ct:fail(no_row_2) end,
    receive {handler, result, {ok, <<"SELECT 1">>}} -> ok after 5000 -> ct:fail(no_result_2) end,

    % Back in #s_ready{} -- prove the connection is still usable.
    ok = gen_statem:cast(Connection, {query, make_ref(), "select 3"}),
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
    % the handler as `handle_query_result/4`'s `{error, Fields}`, the same callback a
    % successful `{ok, Tag}` goes through.
    ok = gen_statem:cast(Connection, {query, make_ref(), "select * from this_table_does_not_exist"}),
    receive
        {handler, result, {error, Error}} ->
            ?assertMatch(#{code := <<"42P01">>}, Error)
    after 5000 ->
        ct:fail(no_error)
    end,
    ?assert(is_process_alive(Connection)),

    % Not fatal -- the connection is still usable afterward.
    ok = gen_statem:cast(Connection, {query, make_ref(), "select 1"}),
    receive {handler, row, _, [<<"1">>]} -> ok after 5000 -> ct:fail(no_row) end,

    ok = pgc_connection:stop(Connection).

listen_notify_test(Config) ->
    {ok, Listener} = pgc_connection:start_link(?MODULE, self(), connection_options(Config, #{})),
    receive
        {handler, ready, _} -> ok
    after 5000 ->
        ct:fail(no_ready_event)
    end,
    ok = gen_statem:cast(Listener, {query, make_ref(), "listen pgc_test_channel"}),
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
    ok = gen_statem:cast(Notifier, {query, make_ref(), "notify pgc_test_channel, 'hello'"}),

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
    ok = gen_statem:cast(Connection, {query, make_ref(), "select pg_sleep(0.2)"}),
    ok = gen_statem:cast(Connection, {query, make_ref(), "select 42 as answer"}),

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
    ok = gen_statem:cast(Connection, {query, make_ref(), "do $$ begin raise notice 'ping'; end $$"}),

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

    ok = gen_statem:cast(Connection, {prepare, make_ref(), ~"s1", "select $1::int4 as n"}),
    receive
        {handler, prepared, {ok, ~"s1", Description}} ->
            ?assertMatch(
                #{parameters_description := [_], row_description := [#pgc_protocol_message:row_description_field{name = <<"n">>}]},
                Description
            )
    after 5000 ->
        ct:fail(no_prepared)
    end,

    ok = gen_statem:cast(Connection, {execute, make_ref(), ~"s1", [{text, <<"42">>}], #{}}),
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

    ok = gen_statem:cast(Connection, {prepare, make_ref(), ~"s1", "select 1 as n"}),
    receive {handler, prepared, {ok, ~"s1", _}} -> ok after 5000 -> ct:fail(no_prepared) end,

    ok = gen_statem:cast(Connection, {execute, make_ref(), ~"s1", [], #{}}),
    receive {handler, row, _, [<<"1">>]} -> ok after 5000 -> ct:fail(no_row) end,
    receive {handler, executed, {ok, _}} -> ok after 5000 -> ct:fail(no_executed) end,

    ok = gen_statem:cast(Connection, {unprepare, make_ref(), ~"s1"}),
    receive {handler, unprepared, {ok, ~"s1"}} -> ok after 5000 -> ct:fail(no_unprepared) end,

    % The statement is gone -- executing it again fails at Bind time (invalid_sql_statement_name),
    % but the connection itself stays healthy.
    ok = gen_statem:cast(Connection, {execute, make_ref(), ~"s1", [], #{}}),
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

    % Same "no separate error callback" shape as `handle_query_result/4` -- a failed
    % `Parse` reaches the handler as `handle_prepare_result/4`'s `{error, Fields}`.
    ok = gen_statem:cast(Connection, {prepare, make_ref(), ~"bad", "not valid sql"}),
    receive
        {handler, prepared, {error, Error}} ->
            ?assertMatch(#{code := <<"42601">>}, Error)
    after 5000 ->
        ct:fail(no_error)
    end,
    ?assert(is_process_alive(Connection)),

    % Not fatal -- the connection is still usable afterward.
    ok = gen_statem:cast(Connection, {query, make_ref(), "select 1"}),
    receive {handler, row, _, [<<"1">>]} -> ok after 5000 -> ct:fail(no_row) end,

    ok = pgc_connection:stop(Connection).

chained_actions_from_one_callback_test(Config) ->
    {ok, Connection} = pgc_connection:start_link(?MODULE, self(), connection_options(Config, #{})),
    receive
        {handler, ready, _Info} -> ok
    after 5000 ->
        ct:fail(no_ready_event)
    end,

    ok = gen_statem:cast(Connection, {prepare, make_ref(), ~"s1", "select 1 as n"}),
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
handle_notice(_ConnectionInfo, Fields, {TestPid, undefined}) ->
    TestPid ! {handler, notice, Fields},
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
handle_row_data(_ConnectionInfo, _Ref, RowDescription, Row, {TestPid, Pending}) ->
    TestPid ! {handler, row, RowDescription, Row},
    {[], {TestPid, Pending}}.

-doc false.
handle_query_result(_ConnectionInfo, _Ref, Result, {TestPid, Pending}) ->
    % `Result` is `empty | {ok, Tag :: binary()}` for a completed statement, or
    % `{error, Fields}` for one that failed -- there's no separate error callback.
    TestPid ! {handler, result, Result},
    {[], {TestPid, Pending}}.

-doc false.
handle_prepare_result(_ConnectionInfo, _Ref, Result, {TestPid, Pending}) ->
    TestPid ! {handler, prepared, Result},
    {[], {TestPid, Pending}}.

-doc false.
handle_unprepare_result(_ConnectionInfo, _Ref, Result, {TestPid, Pending}) ->
    TestPid ! {handler, unprepared, Result},
    {[], {TestPid, Pending}}.

-doc false.
handle_execute_result(_ConnectionInfo, _Ref, Result, {TestPid, Pending}) ->
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
handle_cast(_ConnectionInfo, {query, Ref, Sql}, {TestPid, Pending}) ->
    % No need to check phase/readiness first -- a `{query, _, _}` action that arrives
    % while a statement is already in flight is postponed automatically by
    % `pgc_connection_statem_common` until the connection is back in `#s_ready{}`.
    {[{query, Ref, Sql}], {TestPid, Pending}};
handle_cast(_ConnectionInfo, {cancel, Ref}, {TestPid, Pending}) ->
    {[{cancel, Ref}], {TestPid, Pending}};
handle_cast(_ConnectionInfo, {prepare, Ref, Name, Text}, {TestPid, Pending}) ->
    {[{prepare, Ref, Name, Text}], {TestPid, Pending}};
handle_cast(_ConnectionInfo, {unprepare, Ref, Name}, {TestPid, Pending}) ->
    {[{unprepare, Ref, Name}], {TestPid, Pending}};
handle_cast(_ConnectionInfo, {execute, Ref, Name, Parameters, Options}, {TestPid, Pending}) ->
    {[{execute, Ref, Name, Parameters, Options}], {TestPid, Pending}};
handle_cast(_ConnectionInfo, {reprepare, Name, NewText}, {TestPid, Pending}) ->
    % `chained_actions_from_one_callback_test`'s own trigger -- one callback return
    % batches three primitives, chained purely via the ordinary postpone-until-ready
    % mechanism, no extra wiring needed on either side.
    Ref = make_ref(),
    {[{unprepare, Ref, Name}, {prepare, Ref, Name, NewText}, {execute, Ref, Name, [], #{}}], {TestPid, Pending}}.

-doc false.
handle_info(_ConnectionInfo, _Info, {TestPid, Pending}) ->
    {[], {TestPid, Pending}}.


% ------------------------------------------------------------------------------
% Helpers
% ------------------------------------------------------------------------------

connection_options(Config, Overrides) ->
    maps:merge(#{
        address => #{host => ct:get_config(postgresql_server_host), port => ct:get_config(postgresql_server_port)},
        tls => disable,
        user => ct:get_config(postgresql_server_user),
        password => ct:get_config(postgresql_server_password),
        database => ?config(database, Config),
        ping_interval => infinity
    }, Overrides).

create_database() ->
    Database = <<"pgc_test_", (binary:encode_hex(crypto:strong_rand_bytes(16), lowercase))/binary>>,
    {ok, _, []} = execute_on_server([~"create database ", {identifier, #{value => Database}}]),
    Database.

drop_database(Database) ->
    {ok, _, []} = execute_on_server([~"drop database ", {identifier, #{value => Database}}, ~" with (force)"]),
    ok.

execute_on_server(Template) ->
    {ok, Client} = pgc_client:start_link(connection_options([{database, ct:get_config(postgresql_server_database)}], #{})),
    Result = pgc_client:execute(Client, pgc_statement:text(pgc_statement:new(Template)), []),
    ok = pgc_client:stop(Client),
    Result.

start_backend() ->
    TestPid = self(),
    Backend = spawn_link(fun () ->
        {ok, Listen} = gen_tcp:listen(0, [binary, {active, false}, {ip, {127, 0, 0, 1}}]),
        {ok, Port} = inet:port(Listen),
        TestPid ! {backend, self(), port, Port},
        {ok, Socket} = gen_tcp:accept(Listen),
        {ok, <<StartupSize:32/integer>>} = gen_tcp:recv(Socket, 4),
        {ok, _Startup} = gen_tcp:recv(Socket, StartupSize - 4),
        ok = gen_tcp:send(Socket, [backend_message($R, <<0:32>>), backend_message($Z, <<"I">>)]),
        serve_backend(TestPid, Socket, first_sync)
    end),
    receive
        {backend, Backend, port, Port} -> {Backend, Port}
    end.

serve_backend(TestPid, Socket, Sync) ->
    case gen_tcp:recv(Socket, 5) of
        {ok, <<$S, 4:32/integer>>} when Sync =:= first_sync ->
            ok = gen_tcp:send(Socket, backend_message($N, <<"SNOTICE", 0, "VNOTICE", 0, "C00000", 0, "Mping", 0, 0>>)),
            TestPid ! {backend, notice_sent},
            receive {backend, ready_for_query} -> ok end,
            ok = gen_tcp:send(Socket, backend_message($Z, <<"I">>)),
            serve_backend(TestPid, Socket, later_sync);
        {ok, <<$S, 4:32/integer>>} when Sync =:= later_sync ->
            ok = gen_tcp:send(Socket, backend_message($Z, <<"I">>)),
            serve_backend(TestPid, Socket, Sync);
        {ok, <<$Q, QuerySize:32/integer>>} ->
            {ok, _Query} = gen_tcp:recv(Socket, QuerySize - 4),
            ok = gen_tcp:send(Socket, [
                backend_message($T, <<1:16, "n", 0, 0:32, 0:16, 23:32, 4:16, -1:32, 0:16>>),
                backend_message($D, <<1:16, 1:32, "1">>),
                backend_message($C, <<"SELECT 1", 0>>),
                backend_message($Z, <<"I">>)
            ]),
            serve_backend(TestPid, Socket, Sync);
        {ok, <<$X, 4:32/integer>>} ->
            gen_tcp:close(Socket);
        {error, closed} ->
            ok
    end.

backend_message(Code, Payload) ->
    <<Code, (byte_size(Payload) + 4):32/integer, Payload/binary>>.

start_cancelling_backend() ->
    TestPid = self(),
    Backend = spawn_link(fun () ->
        {ok, Listen} = gen_tcp:listen(0, [binary, {active, false}, {ip, {127, 0, 0, 1}}]),
        {ok, Port} = inet:port(Listen),
        TestPid ! {backend, self(), port, Port},
        {ok, Socket} = gen_tcp:accept(Listen),
        {ok, <<StartupSize:32/integer>>} = gen_tcp:recv(Socket, 4),
        {ok, _Startup} = gen_tcp:recv(Socket, StartupSize - 4),
        ok = gen_tcp:send(Socket, [backend_message($R, <<0:32>>), backend_message($K, <<1:32, 2:32>>), backend_message($Z, <<"I">>)]),
        {ok, <<$Q, QuerySize:32/integer>>} = gen_tcp:recv(Socket, 5),
        {ok, _Query} = gen_tcp:recv(Socket, QuerySize - 4),
        ok = accept_cancel_request(Listen),
        TestPid ! {backend, cancel_ignored},
        ok = accept_cancel_request(Listen),
        ok = gen_tcp:send(Socket, [
            backend_message($E, <<"SERROR", 0, "VERROR", 0, "C57014", 0, "Mcanceling statement due to user request", 0, 0>>),
            backend_message($Z, <<"I">>)
        ]),
        case gen_tcp:recv(Socket, 5) of
            {ok, <<$X, 4:32/integer>>} -> gen_tcp:close(Socket);
            {error, closed} -> ok
        end
    end),
    receive
        {backend, Backend, port, Port} -> Port
    end.

accept_cancel_request(Listen) ->
    {ok, Socket} = gen_tcp:accept(Listen),
    {ok, <<16:32/integer, 1234:16/integer, 5678:16/integer, 1:32/integer, 2:32/integer>>} = gen_tcp:recv(Socket, 16),
    gen_tcp:close(Socket).
