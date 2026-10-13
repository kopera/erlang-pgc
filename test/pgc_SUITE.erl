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
    ?assertError({pgc, in_transaction}, pgc:transaction(Pool, fun (_Tx) ->
        {commit, pgc:transaction(Pool, fun (_) -> {commit, unreachable} end)}
    end)).

execute_stale_transaction_ref_test(Config) ->
    {ok, Pool} = pgc:start_link(client_options(Config, #{}), #{max_size => 1}),
    StaleRef = pgc:transaction(Pool, fun (Tx) -> {commit, Tx} end),
    ?assertError({pgc, not_in_transaction}, pgc:execute(StaleRef, "select 1")).


% ------------------------------------------------------------------------------
% Helpers
% ------------------------------------------------------------------------------

client_options(Config, Overrides) ->
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
    {ok, Client} = pgc_client:start_link(client_options([], #{database => ct:get_config(postgresql_server_database)})),
    Result = pgc_client:execute(Client, pgc_statement:text(pgc_statement:new(Template)), []),
    ok = pgc_client:stop(Client),
    Result.
