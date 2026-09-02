-module(pgc_protocol_data_type).
-moduledoc false.
-export([
    catalog_statement_name/0,
    catalog_statement_text/0,
    catalog_result_formats/0,
    decode_row/1
]).
-export_record([
    descriptor
]).
-export_type([
    oid/0,
    descriptor/0
]).

% ------------------------------------------------------------------------------
% Types
% ------------------------------------------------------------------------------

-record #descriptor{
    oid :: oid(),
    namespace :: name(),
    name :: name(),
    kind :: kind(),
    send :: name(),
    recv :: name(),
    element :: oid() | undefined,
    parent :: oid() | undefined,
    fields :: [{name(), oid()}]
}.
-type oid() :: 1..4294967295.
-type name() :: unicode:unicode_binary().
-type kind() :: base | composite | domain | enum | pseudo | range | multirange | other.
-type descriptor() :: #descriptor{}.

% ------------------------------------------------------------------------------
% API
% ------------------------------------------------------------------------------

-doc "The name the catalog statement is `Parse`d under; internal, never handed to a caller.".
-spec catalog_statement_name() -> binary().
catalog_statement_name() ->
    ~"_pgc_connection_:refresh_types".

-doc "The catalog statement's SQL text.".
-spec catalog_statement_text() -> binary().
catalog_statement_text() ->
    ~"""
    select
        pg_type.oid as oid,
        pg_namespace.nspname as namespace,
        pg_type.typname as name,
        pg_type.typtype as type,
        pg_type.typsend as send,
        pg_type.typreceive as recv,
        pg_type.typelem as element_type,
        coalesce(pg_range.rngsubtype, 0) as parent_type,
        array (
            select pg_attribute.attname
            from pg_attribute
            where pg_attribute.attrelid = pg_type.typrelid
            and pg_attribute.attnum > 0
            and not pg_attribute.attisdropped
            order by pg_attribute.attnum
        ) as fields_names,
        array (
            select pg_attribute.atttypid
            from pg_attribute
            where pg_attribute.attrelid = pg_type.typrelid
            and pg_attribute.attnum > 0
            and not pg_attribute.attisdropped
            order by pg_attribute.attnum
        ) as fields_types
    from pg_catalog.pg_type
    left join pg_catalog.pg_range on pg_range.rngtypid = pg_type.oid
    left join pg_catalog.pg_namespace on pg_namespace.oid = pg_type.typnamespace
    """.

-doc """
The per-column result formats to `Bind` the catalog statement with, in
column order. `send`/`recv` (columns 5/6) are requested as `text` rather than
`binary` -- since they are `regproc`, the text wire format is the function's
name, sparing a second lookup that binary format (a raw oid) would require.
""".
-spec catalog_result_formats() -> [text | binary, ...].
catalog_result_formats() ->
    [binary, binary, binary, binary, text, text, binary, binary, binary, binary].

-doc "Decodes one `#data_row{}` produced by the catalog statement into a `type()`.".
-spec decode_row([binary()]) -> descriptor().
decode_row([Oid, Namespace, Name, Kind, Send, Recv, Element, Parent, FieldsNames, FieldsTypes]) ->
    #descriptor{
        oid = decode_oid(Oid),
        namespace = decode_name(Namespace),
        name = decode_name(Name),
        kind = decode_kind(decode_char(Kind)),
        send = decode_name(Send),
        recv = decode_name(Recv),
        element = decode_optional_oid(Element),
        parent = decode_optional_oid(Parent),
        fields = decode_fields(FieldsNames, FieldsTypes)
    }.


% ------------------------------------------------------------------------------
% Helpers
% ------------------------------------------------------------------------------

-spec decode_oid(binary()) -> oid().
decode_oid(<<Value:32/integer>>) when Value =/= 0 ->
    Value.

-spec decode_optional_oid(binary()) -> oid() | undefined.
decode_optional_oid(<<Value:32>>) when Value =/= 0 ->
    Value;
decode_optional_oid(<<0:32>>)->
    undefined.

-spec decode_name(binary()) -> name().
decode_name(Value) when is_binary(Value) ->
    Value.

-spec decode_char(binary()) -> byte().
decode_char(<<Char>>) ->
    Char.

-spec decode_kind(byte()) -> kind().
decode_kind($b) -> base;
decode_kind($c) -> composite;
decode_kind($d) -> domain;
decode_kind($e) -> enum;
decode_kind($p) -> pseudo;
decode_kind($r) -> range;
decode_kind($m) -> multirange;
decode_kind(_) -> other.

-spec decode_fields(binary(), binary()) -> [{name(), oid()}].
decode_fields(NamesValue, TypesValue) ->
    Names = decode_array(fun decode_name/1, NamesValue),
    Types = decode_array(fun decode_oid/1, TypesValue),
    lists:zip(Names, Types).

-doc """
A minimal binary-format array decoder scoped to this query's 1-dimensional,
never-null-element `text[]`/`oid[]` columns -- not a general array codec.
""".
-spec decode_array(fun((binary()) -> T), binary()) -> [T].
decode_array(_DecodeElement, <<0:32/signed, _Flags:32/signed, _ElementOid:32/signed>>) ->
    [];
decode_array(DecodeElement, <<1:32/signed, _Flags:32/signed, _ElementOid:32/signed, _Length:32/signed, 1:32/signed, Rest/binary>>) ->
    decode_array_elements(DecodeElement, Rest).

decode_array_elements(_DecodeElement, <<>>) ->
    [];
decode_array_elements(DecodeElement, <<Size:32/signed, Data:Size/binary, Rest/binary>>) ->
    [DecodeElement(Data) | decode_array_elements(DecodeElement, Rest)].


% ------------------------------------------------------------------------------
% Tests
% ------------------------------------------------------------------------------

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

empty_array() ->
    <<0:32/signed, 0:32/signed, 0:32/signed>>.

array(ElementOid, Elements) ->
    Header = <<1:32/signed, 0:32/signed, ElementOid:32/signed, (length(Elements)):32/signed, 1:32/signed>>,
    Body = << <<(byte_size(E)):32/signed, E/binary>> || E <- Elements >>,
    <<Header/binary, Body/binary>>.

row(KindByte, ElementOid, ParentOid, Fields) ->
    [
        <<23:32>>,
        ~"pg_catalog",
        ~"int4",
        <<KindByte>>,
        ~"int4send",
        ~"int4recv",
        <<ElementOid:32>>,
        <<ParentOid:32>>,
        case Fields of [] -> empty_array(); _ -> array(25, [N || {N, _} <- Fields]) end,
        case Fields of [] -> empty_array(); _ -> array(26, [<<T:32>> || {_, T} <- Fields]) end
    ].

decode_row_base_type_test() ->
    Descriptor = decode_row(row($b, 0, 0, [])),
    ?assertEqual(23, Descriptor#descriptor.oid),
    ?assertEqual(~"pg_catalog", Descriptor#descriptor.namespace),
    ?assertEqual(~"int4", Descriptor#descriptor.name),
    ?assertEqual(base, Descriptor#descriptor.kind),
    ?assertEqual(~"int4send", Descriptor#descriptor.send),
    ?assertEqual(~"int4recv", Descriptor#descriptor.recv),
    ?assertEqual(undefined, Descriptor#descriptor.element),
    ?assertEqual(undefined, Descriptor#descriptor.parent),
    ?assertEqual([], Descriptor#descriptor.fields).

decode_row_kind_mapping_test() ->
    Cases = [
        {$b, base}, {$c, composite}, {$d, domain}, {$e, enum},
        {$p, pseudo}, {$r, range}, {$m, multirange}, {$x, other}
    ],
    [?assertEqual(Kind, (decode_row(row(Byte, 0, 0, [])))#descriptor.kind) || {Byte, Kind} <- Cases].

decode_row_element_and_parent_test() ->
    Descriptor = decode_row(row($b, 23, 0, [])),
    ?assertEqual(23, Descriptor#descriptor.element),
    ?assertEqual(undefined, Descriptor#descriptor.parent),
    Descriptor2 = decode_row(row($r, 0, 23, [])),
    ?assertEqual(undefined, Descriptor2#descriptor.element),
    ?assertEqual(23, Descriptor2#descriptor.parent).

decode_row_composite_fields_test() ->
    Descriptor = decode_row(row($c, 0, 0, [{~"a", 23}, {~"b", 25}])),
    ?assertEqual([{~"a", 23}, {~"b", 25}], Descriptor#descriptor.fields).

catalog_statement_test() ->
    ?assert(is_binary(catalog_statement_name())),
    ?assert(is_binary(catalog_statement_text())),
    ?assertEqual(10, length(catalog_result_formats())).

-endif.
