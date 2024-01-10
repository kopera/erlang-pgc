-module(pgc_types).
-export([
    new/0,
    add/2
]).
-export([
    lookup/2
]).
-export_type([
    t/0
]).

-include("./pgc_type.hrl").

-opaque t() :: #{
    pgc_type:oid() => pgc_type:t()
}.


-spec new() -> t().
new() ->
    #{}.


-spec add(pgc_type:t(), t()) -> t().
add(#pgc_type{oid = Oid} = Type, Types) ->
    maps:put(Oid, Type, Types).


-spec lookup([pgc_type:oid()], t()) -> {Result, Unknown} when
    Result :: [pgc_type:t()],
    Unknown :: ordsets:ordset(pgc_type:oid()).
lookup(TypeIDs, Types) ->
    lookup(Types, TypeIDs, #{}, ordsets:new()).


%% @private
-spec lookup(t(), [pgc_type:oid()], TypesAcc, MissingAcc) -> {Result, Unknown} when
    TypesAcc :: #{ pgc_type:oid() => pgc_type:t() },
    MissingAcc :: ordsets:ordset(pgc_type:oid()),
    Result :: [pgc_type:t()],
    Unknown :: MissingAcc.
lookup(_Types, [], TypesAcc, MissingAcc) ->
    {maps:values(TypesAcc), MissingAcc};
lookup(Types, [TypeID | Rest], TypesAcc, MissingAcc) when is_map_key(TypeID, TypesAcc) ->
    lookup(Types, Rest, TypesAcc, MissingAcc);
lookup(Types, [TypeID | Rest], TypesAcc, MissingAcc) ->
    case Types of
        #{TypeID := Type} ->
            Dependencies = dependencies(Type),
            lookup(Types, Dependencies ++ Rest, TypesAcc#{TypeID => Type}, MissingAcc);
        #{} ->
            lookup(Types, Rest, TypesAcc, ordsets:add_element(TypeID, MissingAcc))
    end.


%% @private
-spec dependencies(pgc_type:t()) -> [pgc_type:oid()].
dependencies(#pgc_type{element = undefined, parent = undefined, fields = undefined}) ->
    [];
dependencies(#pgc_type{element = ElementType, parent = ParentType, fields = Fields}) ->
    lists:flatmap(fun
        (undefined) -> [];
        (Children) when is_list(Fields) -> [TypeID || {_, TypeID} <- Children];
        (TypeID) when is_integer(TypeID) -> [TypeID]
    end, [ElementType, ParentType, Fields]).