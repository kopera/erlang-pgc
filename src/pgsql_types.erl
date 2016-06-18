-module(pgsql_types).
-export([
    new/0,
    add/2,
    get_query/0
]).
-export_type([
    oid/0,
    types/0
]).

-include("./pgsql_protocol_messages.hrl").
-include("../include/types.hrl").

-type oid() :: 1..4294967295.
-type type() :: #pgsql_type_info{}.
-type types() :: [type()].

new() ->
    [].

add(Row, Types) ->
    [make_type(Row) | Types].

get_query() ->
    Query = "SELECT
        t.oid as oid,
        t.typname as name,
        t.typsend as send,
        t.typreceive as recv,
        t.typelem as element_type,
        coalesce(r.rngsubtype, 0) as parent_type,
        ARRAY (
            SELECT a.attname
            FROM pg_attribute AS a
            WHERE a.attrelid = t.typrelid AND a.attnum > 0 AND NOT a.attisdropped
            ORDER BY a.attnum
        ) as fields_names,
        ARRAY (
            SELECT a.atttypid
            FROM pg_attribute AS a
            WHERE a.attrelid = t.typrelid AND a.attnum > 0 AND NOT a.attisdropped
            ORDER BY a.attnum
        ) as fields_types
    FROM pg_type AS t
    LEFT JOIN pg_range AS r ON r.rngtypid = t.oid",
    {Query, [binary, text, text, text, binary, binary, binary, binary]}.



make_type([Oid, Name, Send, Recv, ElementType, ParentType, FieldsNames, FieldsTypes]) ->
    #pgsql_type_info{
        oid = pgsql_codec_oid:decode(Oid),
        name = binary_to_atom(pgsql_codec_text:decode(Name), utf8),
        send = binary_to_atom(pgsql_codec_text:decode(Send), utf8),
        recv = binary_to_atom(pgsql_codec_text:decode(Recv), utf8),
        element = case pgsql_codec_oid:decode(ElementType) of
            0 -> undefined;
            ElementOid -> ElementOid
        end,
        parent = case pgsql_codec_oid:decode(ParentType) of
            0 -> undefined;
            ParentOid -> ParentOid
        end,
        fields = make_fields(FieldsNames, FieldsTypes)
    }.

make_fields(Names, Types) ->
    case lists:zip(
        pgsql_codec_array:decode(fun (_Oid, V) -> binary_to_atom(pgsql_codec_text:decode(V), utf8) end, Names),
        pgsql_codec_array:decode(fun (_Oid, V) -> pgsql_codec_oid:decode(V) end, Types)
    ) of
        [] -> undefined;
        Other -> Other
    end.
