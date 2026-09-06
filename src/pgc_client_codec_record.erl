-module(pgc_client_codec_record).
-moduledoc false.

-behaviour(pgc_client_codec).
-export([
    names/0,
    encode/3,
    decode/3
]).

names() ->
    [~"record_send", ~"record_recv"].

encode(Term, {_Oid, _Name, _Kind, _Recv, _Send, _Element, _Parent, FieldsDescription}, Types) ->
    Fields = from_term(FieldsDescription, Term),
    FieldsCount = length(Fields),
    [<<FieldsCount:32/integer>> | encode_fields(Fields, Types)].


-doc """
Decode mode comes from this call's `codecs => #{record => #{decode => Mode}}` option -- `map` is
the only mode for now (matching the field-name-keyed map a caller passes in on encode), kept as
an explicit option rather than hardcoded so a later mode (e.g. a positional tuple) doesn't need a
new call shape.
""".
decode(<<_Count:32/integer, Payload/binary>>, {_Oid, _Name, _Kind, _Recv, _Send, _Element, _Parent, FieldsDescription}, Types) ->
    map = maps:get(decode, pgc_client_types:codec_options(record, Types), map),
    Fields = decode_fields(Payload, Types),
    to_term(FieldsDescription, Fields).


% ------------------------------------------------------------------------------
% Encoding
% ------------------------------------------------------------------------------

from_term(FieldsDescription, Map) when FieldsDescription =/= undefined, is_map(Map) ->
    [{Oid, maps:get(FieldName, Map, null)} || {FieldName, Oid} <- FieldsDescription];
from_term(FieldsDescription, Value) ->
    erlang:error(badarg, [FieldsDescription, Value]).

encode_fields(Fields, Types) ->
    [encode_field(Oid, FieldValue, Types) || {Oid, FieldValue} <- Fields].

encode_field(Oid, null, _Types) ->
    <<Oid:32/integer, -1:32/signed-integer>>;
encode_field(Oid, Value, Types) ->
    {ok, Descriptor} = pgc_client_types:lookup(Oid, Types),
    Encoded = pgc_client_codec:encode(Value, Descriptor, Types),
    [<<Oid:32/integer, (iolist_size(Encoded)):32/signed-integer>>, Encoded].


% ------------------------------------------------------------------------------
% Decoding
% ------------------------------------------------------------------------------

to_term(undefined, Fields) ->
    % Anonymous record -- no field names to key by, so number them instead.
    lists:foldl(fun ({_Oid, Value}, Acc) ->
        Key = map_size(Acc) + 1,
        Acc#{Key => Value}
    end, #{}, Fields);
to_term(FieldsDescription, Fields) ->
    TupleList = lists:zipwith(fun ({FieldName, Oid}, {Oid, FieldValue}) ->
        {FieldName, FieldValue}
    end, FieldsDescription, Fields),
    maps:from_list(TupleList).

decode_fields(Data, Types) ->
    decode_fields(Data, Types, []).

decode_fields(<<>>, _Types, Acc) ->
    lists:reverse(Acc);
decode_fields(<<Oid:32/integer, -1:32/signed-integer, Rest/binary>>, Types, Acc) ->
    decode_fields(Rest, Types, [{Oid, null} | Acc]);
decode_fields(<<Oid:32/integer, Size:32/signed-integer, FieldData:Size/binary, Rest/binary>>, Types, Acc) ->
    {ok, Descriptor} = pgc_client_types:lookup(Oid, Types),
    Value = pgc_client_codec:decode(FieldData, Descriptor, Types),
    decode_fields(Rest, Types, [{Oid, Value} | Acc]).
