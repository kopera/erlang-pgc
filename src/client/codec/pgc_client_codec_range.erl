-module(pgc_client_codec_range).
-moduledoc false.

-export([
    encode/3,
    decode/3
]).
-export_type([
    range/0,
    bound/0
]).

-import_record(pgc_client_codec, [codec]).
-import_record(pgc_client_types, [descriptor]).

-type bound() :: unbound | {inclusive, term()} | {exclusive, term()}.
-type range() :: empty | #{lower := bound(), upper := bound()}.

-define(empty, 16#01).
-define(lb_inclusive, 16#02).
-define(ub_inclusive, 16#04).
-define(lb_infinite, 16#08).
-define(ub_infinite, 16#10).


-spec encode(Value, Descriptor, Codec) -> iodata() when
    Value :: range(),
    Descriptor :: pgc_client_types:descriptor(),
    Codec :: #codec{}.
encode(empty, _Descriptor, _Codec) ->
    <<?empty:8>>;
encode(#{lower := Lower, upper := Upper}, #descriptor{parent = ElementTypeId}, Codec) ->
    EncodeElement = fun(Value) -> pgc_client_codec:encode(ElementTypeId, Value, Codec) end,
    {LowerFlags, LowerData} = encode_bound(Lower, ?lb_inclusive, ?lb_infinite, EncodeElement),
    {UpperFlags, UpperData} = encode_bound(Upper, ?ub_inclusive, ?ub_infinite, EncodeElement),
    [<<(LowerFlags bor UpperFlags):8>>, LowerData, UpperData];
encode(Value, Descriptor, Codec) ->
    erlang:error(badarg, [Value, Descriptor, Codec]).

encode_bound(unbound, _InclusiveFlag, InfiniteFlag, _EncodeElement) ->
    {InfiniteFlag, <<>>};
encode_bound({inclusive, Value}, InclusiveFlag, _InfiniteFlag, EncodeElement) ->
    {InclusiveFlag, encode_bound_value(Value, EncodeElement)};
encode_bound({exclusive, Value}, _InclusiveFlag, _InfiniteFlag, EncodeElement) ->
    {0, encode_bound_value(Value, EncodeElement)}.

encode_bound_value(Value, EncodeElement) ->
    Encoded = EncodeElement(Value),
    [<<(iolist_size(Encoded)):32/signed-integer>>, Encoded].


-spec decode(Data, Descriptor, Codec) -> range() when
    Data :: binary(),
    Descriptor :: pgc_client_types:descriptor(),
    Codec :: #codec{}.
decode(<<Flags:8>>, _Descriptor, _Codec) when Flags band ?empty =/= 0 ->
    empty;
decode(<<Flags:8, Rest/binary>>, #descriptor{parent = ElementTypeId}, Codec) ->
    DecodeElement = fun(Data) -> pgc_client_codec:decode(ElementTypeId, Data, Codec) end,
    {Lower, Rest1} = decode_bound(Flags, ?lb_infinite, ?lb_inclusive, Rest, DecodeElement),
    {Upper, <<>>} = decode_bound(Flags, ?ub_infinite, ?ub_inclusive, Rest1, DecodeElement),
    #{lower => Lower, upper => Upper}.

decode_bound(Flags, InfiniteFlag, _InclusiveFlag, Data, _DecodeElement) when Flags band InfiniteFlag =/= 0 ->
    {unbound, Data};
decode_bound(Flags, _InfiniteFlag, InclusiveFlag, <<Size:32/signed-integer, ValueData:Size/binary, Rest/binary>>, DecodeElement) ->
    Value = DecodeElement(ValueData),
    Bound = case Flags band InclusiveFlag of 0 -> {exclusive, Value}; _ -> {inclusive, Value} end,
    {Bound, Rest}.