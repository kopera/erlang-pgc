-module(pgc_client_codec_record).
-moduledoc false.

-export([
    encode/3,
    decode/3
]).

-import_record(pgc_client_codec, [codec]).
-import_record(pgc_client_types, [descriptor]).


-spec encode(Values, Descriptor, Codec) -> iodata() when
    Values :: list(),
    Descriptor :: pgc_client_types:descriptor(),
    Codec :: #codec{}.
encode(Record, #descriptor{fields = FieldsDescription}, Codec) when FieldsDescription =/= undefined ->
    Fields = [{TypeId, maps:get(Name, Record, null)} || {Name, TypeId} <- FieldsDescription],
    FieldsCount = length(Fields),
    EncodeField = fun(TypeId, Value) -> pgc_client_codec:encode(TypeId, Value, Codec) end,
    [<<FieldsCount:32/integer>> | encode_fields(EncodeField, Fields)].


-spec decode(Data, Descriptor, Codec) -> map() when
    Data :: binary(),
    Descriptor :: pgc_client_types:descriptor(),
    Codec :: #codec{}.
decode(<<_Count:32/integer, Payload/binary>>, #descriptor{fields = FieldsDescription}, Codec) ->
    DecodeField = fun(TypeId, Data) -> pgc_client_codec:decode(TypeId, Data, Codec) end,
    Fields = decode_fields(DecodeField, Payload),
    to_term(FieldsDescription, Fields).


% ------------------------------------------------------------------------------
% Encoding
% ------------------------------------------------------------------------------

encode_fields(EncodeField, Fields) ->
    [encode_field(EncodeField, TypeId, Value) || {TypeId, Value} <- Fields].

encode_field(_EncodeField, TypeId, null) ->
    <<TypeId:32/integer, -1:32/signed-integer>>;
encode_field(EncodeField, TypeId, Value) ->
    Encoded = EncodeField(TypeId, Value),
    [<<TypeId:32/integer, (iolist_size(Encoded)):32/signed-integer>>, Encoded].


% ------------------------------------------------------------------------------
% Decoding
% ------------------------------------------------------------------------------

decode_fields(_DecodeField, <<>>) ->
    [];
decode_fields(DecodeField, <<TypeId:32/integer, -1:32/signed-integer, Rest/binary>>) ->
    [{TypeId, null} | decode_fields(DecodeField, Rest)];
decode_fields(DecodeField, <<TypeId:32/integer, Size:32/signed-integer, FieldData:Size/binary, Rest/binary>>) ->
    Value = DecodeField(TypeId, FieldData),
    [{TypeId, Value} | decode_fields(DecodeField, Rest)].


to_term(undefined, Fields) ->
    % Anonymous record -- no field names to key by, so number them instead.
    lists:foldl(fun ({_Oid, Value}, Acc) ->
        Key = map_size(Acc) + 1,
        Acc#{Key => Value}
    end, #{}, Fields);
to_term(FieldsDescription, Fields) ->
    #{
        Name => Value || {Name, TypeId} <:- FieldsDescription && {TypeId, Value} <:- Fields
    }.

