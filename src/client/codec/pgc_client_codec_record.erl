-module(pgc_client_codec_record).
-moduledoc false.

-export([
    encode/3,
    decode/3
]).

encode(Term, {_Oid, _Name, _Kind, _Recv, _Send, _Element, _Parent, FieldsDescription}, Codecs) ->
    Fields = from_term(FieldsDescription, Term),
    FieldsCount = length(Fields),
    [<<FieldsCount:32/integer>> | encode_fields(Fields, Codecs)].


-doc """
Decode mode comes from this call's `codecs => #{record => #{decode => Mode}}` option -- `map` is
the only mode for now (matching the field-name-keyed map a caller passes in on encode), kept as
an explicit option rather than hardcoded so a later mode (e.g. a positional tuple) doesn't need a
new call shape.
""".
decode(<<_Count:32/integer, Payload/binary>>, {_Oid, _Name, _Kind, _Recv, _Send, _Element, _Parent, FieldsDescription}, Codecs) ->
    map = maps:get(decode, pgc_client_codec:options(record, Codecs), map),
    Fields = decode_fields(Payload, Codecs),
    to_term(FieldsDescription, Fields).


% ------------------------------------------------------------------------------
% Encoding
% ------------------------------------------------------------------------------

from_term(FieldsDescription, Map) when FieldsDescription =/= undefined, is_map(Map) ->
    [{Oid, maps:get(FieldName, Map, null)} || {FieldName, Oid} <- FieldsDescription];
from_term(FieldsDescription, Value) ->
    erlang:error(badarg, [FieldsDescription, Value]).

encode_fields(Fields, Codecs) ->
    [encode_field(Oid, FieldValue, Codecs) || {Oid, FieldValue} <- Fields].

encode_field(Oid, null, _Codecs) ->
    <<Oid:32/integer, -1:32/signed-integer>>;
encode_field(Oid, Value, Codecs) ->
    {ok, Descriptor} = pgc_client_codec:lookup(Oid, Codecs),
    Encoded = pgc_client_codec:encode(Value, Descriptor, Codecs),
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

decode_fields(Data, Codecs) ->
    decode_fields(Data, Codecs, []).

decode_fields(<<>>, _Codecs, Acc) ->
    lists:reverse(Acc);
decode_fields(<<Oid:32/integer, -1:32/signed-integer, Rest/binary>>, Codecs, Acc) ->
    decode_fields(Rest, Codecs, [{Oid, null} | Acc]);
decode_fields(<<Oid:32/integer, Size:32/signed-integer, FieldData:Size/binary, Rest/binary>>, Codecs, Acc) ->
    {ok, Descriptor} = pgc_client_codec:lookup(Oid, Codecs),
    Value = pgc_client_codec:decode(FieldData, Descriptor, Codecs),
    decode_fields(Rest, Codecs, [{Oid, Value} | Acc]).
