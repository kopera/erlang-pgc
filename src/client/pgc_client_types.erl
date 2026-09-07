-module(pgc_client_types).
-moduledoc false.

-export([
    new/0,
    add/3,
    has/2,
    lookup/2
]).
-export_record([
    descriptor
]).
-export_type([
    t/0,
    id/0,
    info/0,
    descriptor/0
]).

-opaque t() :: ets:table().

-record #descriptor{
    namespace :: binary(),
    name :: binary(),
    kind :: kind(),
    recv :: binary(),
    send :: binary(),
    element :: id() | undefined,
    parent :: id() | undefined,
    fields :: [{binary(), id()}] | undefined
}.

-type id() :: pgc_protocol:oid().
-type info() :: #{
    namespace := binary(),
    name := binary(),
    kind := kind(),
    recv := binary(),
    send := binary(),
    element => id() | undefined,
    parent => id() | undefined,
    fields => [{binary(), id()}] | undefined
}.
-type kind() :: base | composite | domain | enum | pseudo | range | multirange | other.
-type descriptor() :: #descriptor{}.


-doc """
Seeds the handful of built-in types `pgc_client`'s own type-refresh query needs to decode its
*own* result columns (`oid`, `text`, and their array forms) -- otherwise decoding that query's
rows would need type descriptors only that same query can ever provide.
""".
-spec new() -> t().
new() ->
    Types = ets:new(?MODULE, [protected, {keypos, 1}]),
    ok = add(26, #{namespace => ~"pg_catalog", name => ~"oid", kind => base, send => ~"oidsend", recv => ~"oidrecv"}, Types),
    ok = add(1028, #{namespace => ~"pg_catalog", name => ~"_oid", kind => base, send => ~"array_send", recv => ~"array_recv", element => 26}, Types),
    ok = add(25, #{namespace => ~"pg_catalog", name => ~"text", kind => base, send => ~"textsend", recv => ~"textrecv"}, Types),
    ok = add(1009, #{namespace => ~"pg_catalog", name => ~"_text", kind => base, send => ~"array_send", recv => ~"array_recv", element => 25}, Types),
    Types.


-spec add(TypeId, TypeInfo, t()) -> ok when
    TypeId :: id(),
    TypeInfo :: info().
add(Id, #{namespace := Namespace, name := Name, kind := Kind, recv := Recv, send := Send} = Info, Types) ->
    true = ets:insert(Types, {
        Id,
        #descriptor{
            namespace = Namespace,
            name = Name,
            kind = Kind,
            recv = Recv,
            send = Send,
            element = maps:get(element, Info, undefined),
            parent = maps:get(parent, Info, undefined),
            fields = maps:get(fields, Info, undefined)
        }
    }),
    ok.


-spec has(id(), t()) -> boolean().
has(Id, Types) ->
    ets:member(Types, Id).


-spec lookup(id(), t()) -> {ok, descriptor()} | error.
lookup(Id, Types) ->
    case ets:lookup(Types, Id) of
        [{_Id, Descriptor}] ->
            {ok, Descriptor};
        [] ->
            error
    end.
