-module(pgc_client_codec_range).
-moduledoc false.

-export([
    encode/3,
    decode/3,
    encode_range/3,
    decode_range/3
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

encode(Range, {_Oid, _Name, _Kind, _Recv, _Send, _Element, Parent, _Fields}, Codecs) ->
    {ok, ElementDescriptor} = pgc_client_codec:lookup(Parent, Codecs),
    encode_range(Range, ElementDescriptor, Codecs).

decode(Data, {_Oid, _Name, _Kind, _Recv, _Send, _Element, Parent, _Fields}, Codecs) ->
    {ok, ElementDescriptor} = pgc_client_codec:lookup(Parent, Codecs),
    {Range, <<>>} = decode_range(Data, ElementDescriptor, Codecs),
    Range.


% ------------------------------------------------------------------------------
% Shared with the multirange codec, whose elements are ranges laid out exactly
% like this (flag byte + optional length-prefixed bounds), one after another.
% ------------------------------------------------------------------------------

-spec encode_range(range(), pgc_client_types:descriptor(), pgc_client_codec:t()) -> iodata().
encode_range(empty, _ElementDescriptor, _Codecs) ->
    <<?empty:8>>;
encode_range(#{lower := Lower, upper := Upper}, ElementDescriptor, Codecs) ->
    {LowerFlags, LowerData} = encode_bound(Lower, ?lb_inclusive, ?lb_infinite, ElementDescriptor, Codecs),
    {UpperFlags, UpperData} = encode_bound(Upper, ?ub_inclusive, ?ub_infinite, ElementDescriptor, Codecs),
    [<<(LowerFlags bor UpperFlags):8>>, LowerData, UpperData];
encode_range(Value, ElementDescriptor, Codecs) ->
    erlang:error(badarg, [Value, ElementDescriptor, Codecs]).

encode_bound(unbound, _InclusiveFlag, InfiniteFlag, _ElementDescriptor, _Codecs) ->
    {InfiniteFlag, <<>>};
encode_bound({inclusive, Value}, InclusiveFlag, _InfiniteFlag, ElementDescriptor, Codecs) ->
    {InclusiveFlag, encode_bound_value(Value, ElementDescriptor, Codecs)};
encode_bound({exclusive, Value}, _InclusiveFlag, _InfiniteFlag, ElementDescriptor, Codecs) ->
    {0, encode_bound_value(Value, ElementDescriptor, Codecs)}.

encode_bound_value(Value, ElementDescriptor, Codecs) ->
    Encoded = pgc_client_codec:encode(Value, ElementDescriptor, Codecs),
    [<<(iolist_size(Encoded)):32/signed-integer>>, Encoded].


-spec decode_range(binary(), pgc_client_types:descriptor(), pgc_client_codec:t()) -> {range(), binary()}.
decode_range(<<Flags:8, Rest/binary>>, _ElementDescriptor, _Codecs) when Flags band ?empty =/= 0 ->
    {empty, Rest};
decode_range(<<Flags:8, Rest/binary>>, ElementDescriptor, Codecs) ->
    {Lower, Rest1} = decode_bound(Flags, ?lb_infinite, ?lb_inclusive, Rest, ElementDescriptor, Codecs),
    {Upper, Rest2} = decode_bound(Flags, ?ub_infinite, ?ub_inclusive, Rest1, ElementDescriptor, Codecs),
    {#{lower => Lower, upper => Upper}, Rest2}.

decode_bound(Flags, InfiniteFlag, _InclusiveFlag, Data, _ElementDescriptor, _Codecs) when Flags band InfiniteFlag =/= 0 ->
    {unbound, Data};
decode_bound(Flags, _InfiniteFlag, InclusiveFlag, <<Size:32/signed-integer, ValueData:Size/binary, Rest/binary>>, ElementDescriptor, Codecs) ->
    Value = pgc_client_codec:decode(ValueData, ElementDescriptor, Codecs),
    Bound = case Flags band InclusiveFlag of 0 -> {exclusive, Value}; _ -> {inclusive, Value} end,
    {Bound, Rest}.
