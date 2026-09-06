-module(pgc_client_codec_multirange).
-moduledoc false.

-export([
    encode/2,
    decode/2
]).

-spec encode([pgc_client_codec_range:range()], fun((term()) -> iodata() | null)) -> iodata().
encode(Ranges, EncodeElement) when is_list(Ranges) ->
    [<<(length(Ranges)):32/signed-integer>> |
        [encode_range(Range, EncodeElement) || Range <- Ranges]];
encode(Value, EncodeElement) ->
    erlang:error(badarg, [Value, EncodeElement]).

encode_range(Range, EncodeElement) ->
    Encoded = pgc_client_codec_range:encode(Range, EncodeElement),
    [<<(iolist_size(Encoded)):32/signed-integer>>, Encoded].


-spec decode(binary(), fun((binary()) -> term())) -> [pgc_client_codec_range:range()].
decode(<<Count:32/signed-integer, Data/binary>>, DecodeElement) ->
    decode_ranges(Count, Data, DecodeElement, []).

decode_ranges(0, <<>>, _DecodeElement, Acc) ->
    lists:reverse(Acc);
decode_ranges(Count, <<Size:32/signed-integer, RangeData:Size/binary, Rest/binary>>, DecodeElement, Acc) ->
    {Range, <<>>} = pgc_client_codec_range:decode(RangeData, DecodeElement),
    decode_ranges(Count - 1, Rest, DecodeElement, [Range | Acc]).
