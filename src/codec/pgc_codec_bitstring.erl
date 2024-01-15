-module(pgc_codec_bitstring).

-behaviour(pgc_codec).
-export([
    init/1,
    encode/2,
    decode/2
]).


init(_Options) ->
    Info = #{
        encodes => [bit_send, varbit_send],
        decodes => [bit_send, varbit_send]
    },
    {Info, []}.


encode(Term, _Options) when is_binary(Term) ->
    Size = bit_size(Term),
    <<Size:32/integer, Term/binary>>;
encode(Term, _Options) when is_bitstring(Term) ->
    Size = bit_size(Term),
    Padding = 8 - (Size rem 8),
    <<Size:32/integer, Term/bitstring, 0:Padding>>;
encode(Term, Options) ->
    erlang:error(badarg, [Term, Options]).


decode(<<Size:32/integer, Bits:Size/bits, _/bits>>, _Options) ->
    Bits.