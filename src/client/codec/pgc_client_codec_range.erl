-module(pgc_client_codec_range).
-moduledoc false.

-export([
    encode/2,
    decode/2
]).
-export_type([
    range/0,
    bound/0
]).

-type bound() :: unbound | {inclusive, term()} | {exclusive, term()}.
-type range() :: empty | #{lower := bound(), upper := bound()}.

-define(empty, 16#01).
-define(lb_inclusive, 16#02).
-define(ub_inclusive, 16#04).
-define(lb_infinite, 16#08).
-define(ub_infinite, 16#10).

-spec encode(range(), fun((term()) -> iodata() | null)) -> iodata().
encode(empty, _EncodeElement) ->
    <<?empty:8>>;
encode(#{lower := Lower, upper := Upper}, EncodeElement) ->
    {LowerFlags, LowerData} = encode_bound(Lower, ?lb_inclusive, ?lb_infinite, EncodeElement),
    {UpperFlags, UpperData} = encode_bound(Upper, ?ub_inclusive, ?ub_infinite, EncodeElement),
    [<<(LowerFlags bor UpperFlags):8>>, LowerData, UpperData];
encode(Value, EncodeElement) ->
    erlang:error(badarg, [Value, EncodeElement]).

encode_bound(unbound, _InclusiveFlag, InfiniteFlag, _EncodeElement) ->
    {InfiniteFlag, <<>>};
encode_bound({inclusive, Value}, InclusiveFlag, _InfiniteFlag, EncodeElement) ->
    {InclusiveFlag, encode_bound_value(Value, EncodeElement)};
encode_bound({exclusive, Value}, _InclusiveFlag, _InfiniteFlag, EncodeElement) ->
    {0, encode_bound_value(Value, EncodeElement)}.

encode_bound_value(Value, EncodeElement) ->
    Encoded = EncodeElement(Value),
    [<<(iolist_size(Encoded)):32/signed-integer>>, Encoded].


-spec decode(binary(), fun((binary()) -> term())) -> {range(), binary()}.
decode(<<Flags:8, Rest/binary>>, _DecodeElement) when Flags band ?empty =/= 0 ->
    {empty, Rest};
decode(<<Flags:8, Rest/binary>>, DecodeElement) ->
    {Lower, Rest1} = decode_bound(Flags, ?lb_infinite, ?lb_inclusive, Rest, DecodeElement),
    {Upper, Rest2} = decode_bound(Flags, ?ub_infinite, ?ub_inclusive, Rest1, DecodeElement),
    {#{lower => Lower, upper => Upper}, Rest2}.

decode_bound(Flags, InfiniteFlag, _InclusiveFlag, Data, _DecodeElement) when Flags band InfiniteFlag =/= 0 ->
    {unbound, Data};
decode_bound(Flags, _InfiniteFlag, InclusiveFlag, <<Size:32/signed-integer, ValueData:Size/binary, Rest/binary>>, DecodeElement) ->
    Value = DecodeElement(ValueData),
    Bound = case Flags band InclusiveFlag of 0 -> {exclusive, Value}; _ -> {inclusive, Value} end,
    {Bound, Rest}.
