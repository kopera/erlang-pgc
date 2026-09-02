-module(pgc_connection_SUITE).
-moduledoc false.

-behaviour(ct_suite).
-export([
    suite/0,
    init_per_suite/1,
    end_per_suite/1,
    init_per_testcase/2,
    end_per_testcase/2,
    all/0
]).

-export([
    connects_and_reaches_ready_test/1,
    wrong_password_stops_with_error_test/1,
    owner_down_stops_cleanly_test/1,
    ping_keepalive_test/1
]).

%% This suite's connection handler: forwards every callback as a tagged message to
%% whatever test process is stashed as the handler's `Args`/`State`.
-behaviour(pgc_connection).
-export([
    init/1,
    handle_connected/2,
    handle_notice/2,
    handle_notification/4,
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
    Config.

-doc false.
end_per_suite(_Config) ->
    ok = application:stop(pgc),
    ok.

-doc "Spawns a throwaway PostgreSQL container for each test case, per the project's convention of not depending on any pre-provisioned server.".
init_per_testcase(_Case, Config) ->
    {Name, Port} = start_postgres(),
    [{postgres_name, Name}, {postgres_port, Port} | Config].

-doc false.
end_per_testcase(_Case, Config) ->
    stop_postgres(?config(postgres_name, Config)),
    ok.

-doc false.
all() ->
    [
        connects_and_reaches_ready_test,
        wrong_password_stops_with_error_test,
        owner_down_stops_cleanly_test,
        ping_keepalive_test
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
    #{backend_key := {BackendId, BackendSecret}, parameters := Parameters} = ConnectionInfo,
    ?assert(is_integer(BackendId)),
    ?assert(is_binary(BackendSecret)),
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
    Name = "pgc-test-" ++ integer_to_list(erlang:unique_integer([positive])),
    Cmd = "docker run --rm -d --name " ++ Name ++
        " -e POSTGRES_USER=" ++ ?POSTGRES_USER ++
        " -e POSTGRES_PASSWORD=" ++ ?POSTGRES_PASSWORD ++
        " -e POSTGRES_DB=" ++ ?POSTGRES_DATABASE ++
        " -p 127.0.0.1::5432 " ++ ?POSTGRES_IMAGE,
    _ = os:cmd(Cmd),
    ok = wait_for_postgres_ready(Name, 120),
    Port = postgres_port(Name),
    {Name, Port}.

wait_for_postgres_ready(Name, 0) ->
    ct:fail({postgres_not_ready, Name});
wait_for_postgres_ready(Name, Retries) ->
    Output = os:cmd("docker exec " ++ Name ++ " pg_isready -U " ++ ?POSTGRES_USER ++ " 2>&1"),
    case string:find(Output, "accepting connections") of
        nomatch ->
            timer:sleep(500),
            wait_for_postgres_ready(Name, Retries - 1);
        _ ->
            ok
    end.

postgres_port(Name) ->
    Output = string:trim(os:cmd("docker port " ++ Name ++ " 5432/tcp")),
    [Line | _] = string:split(Output, "\n"),
    [_Host, PortString] = string:split(Line, ":", trailing),
    erlang:list_to_integer(string:trim(PortString)).

stop_postgres(Name) ->
    _ = os:cmd("docker stop " ++ Name),
    ok.
