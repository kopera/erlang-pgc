-module(pgsql_types).
-export([
    load/1
]).
-export_type([
    oid/0,
    types/0
]).


-include("./pgsql_protocol_messages.hrl").
-include("../include/types.hrl").
-define(recv_timeout, 5000).

-type oid() :: 1..4294967295.
-type type() :: #pgsql_type_info{}.
-type types() :: [type()].

-spec load(pgsql_transport:transport()) -> {types(), pgsql_transport:transport()}.
load(Transport) ->
    StatementName = "erlang-pgsql:" ++ ?MODULE_STRING ++ ":update/2",
    Statement = "SELECT
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
    ok = pgsql_transport:send(Transport, [
        #msg_parse{name = StatementName, statement = Statement},
        #msg_bind{portal = "", statement = StatementName, results = [binary, text, text, text, binary, binary, binary, binary]},
        #msg_close{type = statement, name = StatementName},
        #msg_execute{portal = "", limit = 0},
        #msg_sync{}
    ]),
    {ok, #msg_parse_complete{}, Transport1} = pgsql_transport:recv(Transport, ?recv_timeout),
    {ok, #msg_bind_complete{}, Transport2} = pgsql_transport:recv(Transport1, ?recv_timeout),
    {ok, #msg_close_complete{}, Transport3} = pgsql_transport:recv(Transport2, ?recv_timeout),
    get_types(Transport3, []).

get_types(Transport, Acc) ->
    case pgsql_transport:recv(Transport, ?recv_timeout) of
        {ok, #msg_command_complete{}, Transport1} ->
            {ok, #msg_ready_for_query{}, Transport2} = pgsql_transport:recv(Transport1, ?recv_timeout),
            {lists:reverse(Acc), Transport2};
        {ok, #msg_data_row{values = Row}, Transport1} ->
            get_types(Transport1, [make_type(Row) | Acc])
    end.

make_type([Oid, Name, Send, Recv, ElementType, ParentType, FieldsNames, FieldsTypes]) ->
    #pgsql_type_info{
        oid = pgsql_codec_oid:decode(Oid),
        name = pgsql_codec_text:decode(Name),
        send = pgsql_codec_text:decode(Send),
        recv = pgsql_codec_text:decode(Recv),
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
                pgsql_codec_array:decode(fun (_Oid, V) -> pgsql_codec_text:decode(V) end, Names),
                pgsql_codec_array:decode(fun (_Oid, V) -> pgsql_codec_oid:decode(V) end, Types)
            ) of
        [] -> undefined;
        Other -> Other
    end.
