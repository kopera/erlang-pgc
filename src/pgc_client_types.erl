-module(pgc_client_types).
-moduledoc false.

-export([
    new/0,
    add/3,
    has/2,
    lookup/2
]).
-export_type([
    t/0
]).

-opaque t() :: ets:table().

-type id() :: pgc_protocol:oid().
-type kind() :: base | composite | domain | enum | pseudo | range | multirange | other.
-type descriptor() :: {
    Oid :: id(),
    Name :: binary(),
    Kind :: kind(),
    Recv :: binary(),
    Send :: binary(),
    Element :: id() | undefined,
    Parent :: id() | undefined,
    Fields :: [{binary(), id()}] | undefined
}.

-spec new() -> t().
new() ->
    ets:new(?MODULE, [protected, {keypos, 1}]).


-spec add(TypeId, TypeInfo, t()) -> ok when
    TypeId :: id(),
    TypeInfo :: #{
        namespace := binary(),
        name := binary(),
        kind := kind(),
        recv := binary(),
        send := binary(),
        element => id() | undefined,
        parent => id() | undefined,
        fields => [{binary(), id()}] | undefined
    }.
add(Id, #{name := Name, kind := Kind, recv := Recv, send := Send} = Info, Types) ->
    true = ets:insert(Types, {
        Id,
        Name,
        Kind,
        Recv,
        Send,
        maps:get(element, Info, undefined),
        maps:get(parent, Info, undefined),
        maps:get(fields, Info, undefined)
    }),
    ok.


-spec has(id(), t()) -> boolean().
has(Oid, Table) ->
    ets:member(Table, Oid).


-spec lookup(id(), t()) -> {ok, descriptor()} | error.
lookup(Id, Types) ->
    case ets:lookup(Types, Id) of
        [Type] -> {ok, Type};
        [] -> error
    end.

% % -----------------------------------------------------------------------------
% % Helpers
% % -----------------------------------------------------------------------------

% -doc """
% Parses the `fields` column: a Postgres array literal of `"name:oid"` entries (e.g.
% `{id:23,name:25}`), empty (`{}`) for anything that isn't a composite type.
% """.
% -spec parse_fields(binary()) -> [{binary(), pgc_protocol:oid()}].
% parse_fields(~"{}") ->
%     [];
% parse_fields(Text) ->
%     Inner = binary:part(Text, 1, byte_size(Text) - 2),
%     [begin
%         [Name, OidText] = binary:split(Entry, ~":"),
%         {Name, binary_to_integer(OidText)}
%     end || Entry <- binary:split(Inner, ~",", [global])].
