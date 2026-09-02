-module(pgc_connection_types).
-moduledoc false.
-export([
    new/0,
    add/2,
    find/2,
    find_all/2
]).
-export_type([
    t/0
]).

-on_load(ensure_deps_loaded/0).
ensure_deps_loaded() ->
    {module, _} = code:ensure_loaded(pgc_protocol_data_type),
    ok.


% ------------------------------------------------------------------------------
% Types
% ------------------------------------------------------------------------------

-record #registry{
    types :: #{pgc_protocol_data_type:oid() => pgc_protocol_data_type:descriptor()}
}.
-opaque t() :: #registry{}.


% ------------------------------------------------------------------------------
% API
% ------------------------------------------------------------------------------

-spec new() -> t().
new() ->
    #registry{
        types = #{}
    }.


-spec add(pgc_protocol_data_type:descriptor(), t()) -> t().
add(#pgc_protocol_data_type:descriptor{oid = Oid} = Type, #registry{} = State) ->
    State#registry{
        types = maps:put(Oid, Type, State#registry.types)
    }.


-spec find(pgc_protocol_data_type:oid(), t()) -> {ok, pgc_protocol_data_type:descriptor()} | error.
find(Oid, #registry{} = State) ->
    maps:find(Oid, State#registry.types).


-spec find_all([pgc_protocol_data_type:oid()], t()) -> {ok, Types} | {error, Types, Missing} when
    Types :: #{pgc_protocol_data_type:oid() => pgc_protocol_data_type:descriptor()},
    Missing :: ordsets:ordset(pgc_protocol_data_type:oid()).
find_all(Oids, #registry{} = State) ->
    OidsSet = ordsets:from_list(Oids),
    Types = maps:with(OidsSet, State#registry.types),
    case ordsets:subtract(OidsSet, ordsets:from_list(maps:keys(Types))) of
        [] -> {ok, Types};
        Missing -> {error, Types, Missing}
    end.


% ------------------------------------------------------------------------------
% Tests
% ------------------------------------------------------------------------------

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

sample_type(Oid) ->
    #pgc_protocol_data_type:descriptor{
        oid = Oid,
        namespace = ~"pg_catalog",
        name = ~"int4",
        kind = base,
        send = ~"int4send",
        recv = ~"int4recv",
        element = undefined,
        parent = undefined,
        fields = []
    }.

new_test() ->
    ?assertEqual({error, #{}, [1]}, find_all([1], new())).

add_and_find_test() ->
    Types = add(sample_type(23), new()),
    ?assertEqual({ok, sample_type(23)}, find(23, Types)),
    ?assertEqual(error, find(25, Types)).

add_overwrites_same_oid_test() ->
    Type1 = sample_type(23),
    Type2 = Type1#pgc_protocol_data_type:descriptor{name = ~"renamed"},
    Types = add(Type2, add(Type1, new())),
    ?assertEqual({ok, Type2}, find(23, Types)).

find_all_ok_test() ->
    Types = add(sample_type(25), add(sample_type(23), new())),
    ?assertEqual({ok, #{23 => sample_type(23), 25 => sample_type(25)}}, find_all([23, 25], Types)).

find_all_missing_test() ->
    Types = add(sample_type(23), new()),
    ?assertEqual({error, #{23 => sample_type(23)}, [25]}, find_all([23, 25], Types)).

-endif.
