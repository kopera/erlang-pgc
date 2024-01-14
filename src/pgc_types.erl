-module(pgc_types).
-export([
    new/0,
    add/2,
    find/2,
    find_all/2
]).
-export_type([
    t/0
]).

-include("./pgc_type.hrl").


-record(types, {
    types    :: #{pgc_type:oid() => pgc_type:t()}
}).
-opaque t() :: #types{}.


-spec new() -> t().
new() ->
    #types{
        types = #{}
    }.


-spec add(pgc_type:t(), t()) -> t().
add(#pgc_type{oid = Oid} = Type, #types{} = State) ->
    State#types{
        types = maps:put(Oid, Type, State#types.types)
    }.


-spec find(pgc_type:oid(), t()) -> {ok, pgc_type:t()} | error.
find(Oid, #types{} = State) ->
    maps:find(Oid, State#types.types).


-spec find_all([pgc_type:oid()], t()) -> {ok, Types} | {error, Types, Missing} when
    Types :: #{pgc_type:oid() => pgc_type:t()},
    Missing :: ordsets:ordset(pgc_type:oid()).
find_all(Oids, #types{} = State) ->
    OidsSet = ordsets:from_list(Oids),
    Types = maps:with(OidsSet, State#types.types),
    case ordsets:subtract(OidsSet, ordsets:from_list(maps:keys(Types))) of
        [] -> {ok, Types};
        Missing -> {error, Types, Missing}
    end.

