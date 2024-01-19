-module(pgc_pool_SUITE).
-export([
    suite/0,
    all/0,
    init_per_suite/1,
    end_per_suite/1,
    init_per_testcase/2,
    end_per_testcase/2
]).
-export([
    simple_test/1,
    waiting_test/1
]).

-define(ASSERT, true).
-include_lib("stdlib/include/assert.hrl").


%% @doc https://www.erlang.org/doc/man/ct_suite#Module:suite-0
suite() ->
    [
        {require, address, 'postgresql_server_address'},
        {require, user, 'postgresql_server_user'},
        {require, password, 'postgresql_server_password'},
        {require, database, 'postgresql_server_database'}
    ].

%% @doc https://www.erlang.org/doc/man/ct_suite#Module:init_per_suite-1
init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(pgc),
    Config.

%% @doc https://www.erlang.org/doc/man/ct_suite#Module:end_per_suite-1
end_per_suite(_Config) ->
    ok = application:stop(pgc),
    ok.

%% @doc https://www.erlang.org/doc/man/ct_suite#Module:init_per_testcase-2
init_per_testcase(_Case, Config) ->
    TransportOptions = #{
        address => ct:get_config(address)
    },
    ConnectionOptions = #{
        user => ct:get_config(user),
        password => ct:get_config(password),
        database => ct:get_config(database),
        hibernate_after => 500
    },
    PoolOptions = #{
        limit => 1
    },
    {ok, Pool} = pgc_pool:start_link(TransportOptions, ConnectionOptions, PoolOptions),
    [{pool, Pool} | Config].

%% @doc https://www.erlang.org/doc/man/ct_suite#Module:end_per_testcase-2
end_per_testcase(_Case, _Config) ->
    % Pool = proplists:get_value(pool, Config),
    % pgc_pool:stop(Pool),
    % ?assertEqual(false, is_process_alive(Pool)),
    ok.

%% @doc https://www.erlang.org/doc/man/ct_suite#Module:all-0
all() ->
    [
        simple_test,
        waiting_test
    ].

% ------------------------------------------------------------------------------
% Test cases
% ------------------------------------------------------------------------------

simple_test(Config) ->
    PoolRef = proplists:get_value(pool, Config),
    ?assertMatch(#{starting := 0, available := 0, used := 0, size := 0, limit := 1}, pgc_pool:info(PoolRef)),
    pgc_pool:with_connection(PoolRef, fun (ConnectionPid) ->
        ?assertMatch(#{starting := 0, available := 0, used := 1, size := 1, limit := 1}, pgc_pool:info(PoolRef)),
        Result = pgc_connection:execute(ConnectionPid, "select 'hello Erlang' as message", [], #{}),
        ?assertMatch({ok, #{}, [#{message := <<"hello Erlang">>}]}, Result)
    end),
    ?assertMatch(#{starting := 0, available := 1, used := 0, size := 1, limit := 1}, pgc_pool:info(PoolRef)).


-define(wait(Message, Timeout), receive Message -> ok after Timeout -> ?assert(false, "Timed out") end).
waiting_test(Config) ->
    PoolRef = proplists:get_value(pool, Config),
    ?assertMatch(#{starting := 0, available := 0, used := 0, size := 0, limit := 1}, pgc_pool:info(PoolRef)),

    Parent = self(),
    User1 = spawn_link(fun () ->
        ?wait(start, 100),
        Parent ! {self(), waiting_for_connection},
        Parent ! pgc_pool:with_connection(PoolRef, fun (ConnectionPid) ->
            Parent ! {self(), got_connection},
            ?assertMatch(#{starting := 0, available := 0, used := 1, size := 1, limit := 1}, pgc_pool:info(PoolRef)),
            Result = pgc_connection:execute(ConnectionPid, "select 'hello from User1' as message", [], #{}),
            {ok, #{}, [#{message := Message}]} = Result,
            ?wait(continue, 1000),
            {self(), Message}
        end)
    end),
    User2 = spawn_link(fun () ->
        ?wait(start, 500),
        Parent ! {self(), waiting_for_connection},
        Parent ! pgc_pool:with_connection(PoolRef, fun (ConnectionPid) ->
            Parent ! {self(), got_connection},
            ?assertMatch(#{starting := 0, available := 0, used := 1, size := 1, limit := 1}, pgc_pool:info(PoolRef)),
            Result = pgc_connection:execute(ConnectionPid, "select 'hello from User2' as message", [], #{}),
            {ok, #{}, [#{message := Message}]} = Result,
            ?wait(continue, 1000),
            {self(), Message}
        end)
    end),
    User1 ! start,
    receive {User1, User1Message1} -> ct:log("User1: ~p", [User1Message1]) end,
    receive {User1, User1Message2} -> ct:log("User1: ~p", [User1Message2]) end,
    User2 ! start,
    receive {User2, User2Message1} -> ct:log("User2: ~p", [User2Message1]) end,
    receive
        {User2, got_connection} -> ct:fail("Should not be reached")
    after 200 ->
        ?assertMatch(#{starting := 0, available := 0, used := 1, size := 1, limit := 1}, pgc_pool:info(PoolRef)),
        ?assertEqual({status, waiting}, erlang:process_info(User2, status))
    end,
    User1 ! continue,
    receive {User2, User2Message2} -> ct:log("User2: ~p", [User2Message2]) end,
    User2 ! continue,
    receive {User1, Message1} -> ?assertEqual(Message1, <<"hello from User1">>) end,
    receive {User2, Message2} -> ?assertEqual(Message2, <<"hello from User2">>) end.