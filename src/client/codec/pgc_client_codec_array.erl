-module(pgc_client_codec_array).
-moduledoc false.

-export([
    encode/3,
    decode/2
]).

-spec encode(list(), pgc_protocol:oid(), fun((term()) -> iodata() | null)) -> iodata().
encode(List, ElementOid, EncodeElement) when is_list(List) ->
    {Flags, EncodedElements} = encode_elements(EncodeElement, lists:flatten(List), 0, []),
    [encode_header(ElementOid, Flags, List) | EncodedElements].


-spec decode(binary(), fun((binary()) -> term())) -> list().
decode(Data, DecodeElement) ->
    {Lengths, Rest} = decode_header(Data),
    Elements = decode_elements(DecodeElement, Rest, []),
    unflatten(Lengths, Elements).


% ------------------------------------------------------------------------------
% Encoding
% ------------------------------------------------------------------------------

encode_header(ElementOid, Flags, Value) ->
    Lengths = lengths(Value, []),
    Dims = length(Lengths),
    <<
        Dims:32/signed-integer,
        Flags:32/signed-integer,
        ElementOid:32/signed-integer,
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

encode_elements(_EncodeElement, [], Flags, Acc) ->
    {Flags, lists:reverse(Acc)};
encode_elements(EncodeElement, [null | Rest], Flags, Acc) ->
    encode_elements(EncodeElement, Rest, Flags bor 1, [<<-1:32/signed-integer>> | Acc]);
encode_elements(EncodeElement, [Value | Rest], Flags, Acc) ->
    Encoded = EncodeElement(Value),
    encode_elements(EncodeElement, Rest, Flags, [[<<(iolist_size(Encoded)):32/signed-integer>>, Encoded] | Acc]).


% ------------------------------------------------------------------------------
% Decoding
% ------------------------------------------------------------------------------

-doc "Convert the 1-d elements list into a multi-dimensional list according to the array lengths.".
unflatten([Length | Lengths], Elements) ->
    unflatten(Lengths, split(Length, Elements, []));
unflatten([], [Elements]) ->
    Elements;
unflatten([], []) ->
    [].

-doc "Split a list into sublists of equal size.".
split(_Length, [], Acc) ->
    lists:reverse(Acc);
split(Length, Elements, Acc) ->
    {Chunk, Rest} = lists:split(Length, Elements),
    split(Length, Rest, [Chunk | Acc]).

decode_header(<<Dims:32/signed-integer, _Flags:32/signed-integer, _ElementOid:32/signed-integer, Rest/binary>>) ->
    decode_lengths(Dims, [], Rest).

decode_lengths(0, Lengths, Payload) ->
    {Lengths, Payload};
decode_lengths(Dims, Lengths, <<Length:32/signed-integer, LowerBound:32/signed-integer, Rest/binary>>) ->
    1 = LowerBound,
    decode_lengths(Dims - 1, [Length | Lengths], Rest).

decode_elements(_DecodeElement, <<>>, Acc) ->
    lists:reverse(Acc);
decode_elements(DecodeElement, <<-1:32/signed-integer, Rest/binary>>, Acc) ->
    decode_elements(DecodeElement, Rest, [null | Acc]);
decode_elements(DecodeElement, <<Size:32/signed-integer, Data:Size/binary, Rest/binary>>, Acc) ->
    decode_elements(DecodeElement, Rest, [DecodeElement(Data) | Acc]).
