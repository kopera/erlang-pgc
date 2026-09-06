-module(pgc_client_codec_record).
-moduledoc false.

-export([
    encode/3,
    decode/3
]).

-spec encode(map(), FieldsDescription, fun((pgc_protocol:oid(), term()) -> iodata() | null)) -> iodata() when
    FieldsDescription :: [{unicode:unicode_binary(), pgc_protocol:oid()}] | undefined.
encode(Term, FieldsDescription, EncodeField) ->
    Fields = from_term(FieldsDescription, Term),
    FieldsCount = length(Fields),
    [<<FieldsCount:32/integer>> | encode_fields(EncodeField, Fields)].


-spec decode(binary(), FieldsDescription, fun((pgc_protocol:oid(), binary()) -> term())) -> term() when
    FieldsDescription :: [{unicode:unicode_binary(), pgc_protocol:oid()}] | undefined.
decode(<<_Count:32/integer, Payload/binary>>, FieldsDescription, DecodeField) ->
    Fields = decode_fields(DecodeField, Payload),
    to_term(FieldsDescription, Fields).


% ------------------------------------------------------------------------------
% Encoding
% ------------------------------------------------------------------------------

from_term(FieldsDescription, Map) when FieldsDescription =/= undefined, is_map(Map) ->
    [{Oid, maps:get(FieldName, Map, null)} || {FieldName, Oid} <- FieldsDescription];
from_term(FieldsDescription, Value) ->
    erlang:error(badarg, [FieldsDescription, Value]).

encode_fields(EncodeField, Fields) ->
    [encode_field(EncodeField, Oid, FieldValue) || {Oid, FieldValue} <- Fields].

encode_field(_EncodeField, Oid, null) ->
    <<Oid:32/integer, -1:32/signed-integer>>;
encode_field(EncodeField, Oid, Value) ->
    Encoded = EncodeField(Oid, Value),
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

decode_fields(DecodeField, Data) ->
    decode_fields(DecodeField, Data, []).

decode_fields(_DecodeField, <<>>, Acc) ->
    lists:reverse(Acc);
decode_fields(DecodeField, <<Oid:32/integer, -1:32/signed-integer, Rest/binary>>, Acc) ->
    decode_fields(DecodeField, Rest, [{Oid, null} | Acc]);
decode_fields(DecodeField, <<Oid:32/integer, Size:32/signed-integer, FieldData:Size/binary, Rest/binary>>, Acc) ->
    Value = DecodeField(Oid, FieldData),
    decode_fields(DecodeField, Rest, [{Oid, Value} | Acc]).
