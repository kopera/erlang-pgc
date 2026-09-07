-module(pgc_client_codec_array).
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
encode(Array, #descriptor{element = ElementTypeId}, Codec) when is_list(Array) ->
    EncodeValue = fun(Value) -> pgc_client_codec:encode(ElementTypeId, Value, Codec) end,
    {Flags, EncodedValues} = encode_values(EncodeValue, Array),
    [encode_header(ElementTypeId, Flags, Array) | EncodedValues].


-spec decode(Data, Descriptor, Codec) -> list() when
    Data :: binary(),
    Descriptor :: pgc_client_types:descriptor(),
    Codec :: #codec{}.
decode(Data, #descriptor{element = ElementTypeId}, Codec) ->
    DecodeValue = fun(Datum) -> pgc_client_codec:decode(ElementTypeId, Datum, Codec) end,
    decode(Data, DecodeValue).


-spec decode(Data, DecodeValue) -> [Element] when
    Data :: binary(),
    DecodeValue :: fun((binary()) -> Element).
decode(Data, DecodeValue) ->
    {Lengths, Payload} = decode_header(Data),
    Values = decode_values(DecodeValue, Payload),
    unflatten(Lengths, Values).

% ------------------------------------------------------------------------------
% Encoding
% ------------------------------------------------------------------------------

encode_header(ElementTypeId, Flags, Values) ->
    Lengths = lengths(Values, []),
    Dims = length(Lengths),
    <<
        Dims:32/signed-integer,
        Flags:32/signed-integer,
        ElementTypeId:32/signed-integer,
        << <<Length:32/signed-integer, 1:32/signed-integer>> || Length <- Lengths >>/binary
    >>.

lengths([], []) ->
    [0];
lengths([], Acc) ->
    lists:reverse(Acc);
lengths([H | _] = Value, Acc) when is_list(H) ->
    lengths(H, [length(Value) | Acc]);
lengths(Value, Acc) ->
    lists:reverse([length(Value) | Acc]).

encode_values(EncodeValue, Values) ->
    encode_values(EncodeValue, lists:flatten(Values), 0, []).

encode_values(_EncodeValue, [], Flags, Acc) ->
    {Flags, lists:reverse(Acc)};
encode_values(EncodeValue, [null | Rest], Flags, Acc) ->
    encode_values(EncodeValue, Rest, Flags bor 1, [<<-1:32/signed-integer>> | Acc]);
encode_values(EncodeValue, [Value | Rest], Flags, Acc) ->
    Encoded = EncodeValue(Value),
    encode_values(EncodeValue, Rest, Flags, [[<<(iolist_size(Encoded)):32/signed-integer>>, Encoded] | Acc]).


% ------------------------------------------------------------------------------
% Decoding
% ------------------------------------------------------------------------------

decode_header(<<Dims:32/signed-integer, _Flags:32/signed-integer, _ElementOid:32/signed-integer, Rest/binary>>) ->
    decode_lengths(Dims, [], Rest).


decode_lengths(0, Lengths, Payload) ->
    {Lengths, Payload};
decode_lengths(Dims, Lengths, <<Length:32/signed-integer, LowerBound:32/signed-integer, Rest/binary>>) ->
    1 = LowerBound,
    decode_lengths(Dims - 1, [Length | Lengths], Rest).


decode_values(_DecodeValue, <<>>) ->
    [];
decode_values(DecodeValue, <<-1:32/signed-integer, Rest/binary>>) ->
    [null | decode_values(DecodeValue, Rest)];
decode_values(DecodeValue, <<Size:32/signed-integer, Data:Size/binary, Rest/binary>>) ->
    [DecodeValue(Data) | decode_values(DecodeValue, Rest)].


unflatten([Length | Lengths], Elements) ->
    unflatten(Lengths, split(Length, Elements));
unflatten([], [Elements]) ->
    Elements;
unflatten([], []) ->
    [].


-doc "Split a list into sublists of equal size.".
split(_Length, []) ->
    [];
split(Length, Elements) ->
    {Chunk, Rest} = lists:split(Length, Elements),
    [Chunk | split(Length, Rest)].
