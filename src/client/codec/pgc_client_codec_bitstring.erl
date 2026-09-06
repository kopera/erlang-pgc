-module(pgc_client_codec_bitstring).
-moduledoc false.

-behaviour(pgc_client_codec).
-export([
    names/0,
    encode/3,
    decode/3
]).

names() ->
    [~"bit_send", ~"bit_recv", ~"varbit_send", ~"varbit_recv"].

encode(Value, _TypeDescriptor, _Codecs) when is_bitstring(Value) ->
    Size = bit_size(Value),
    Padding = (8 - (Size rem 8)) rem 8,
    <<Size:32/integer, Value/bitstring, 0:Padding>>;
encode(Value, TypeDescriptor, Codecs) ->
    erlang:error(badarg, [Value, TypeDescriptor, Codecs]).

decode(<<Size:32/integer, Bits:Size/bits, _/bits>>, _TypeDescriptor, _Codecs) ->
    Bits.
