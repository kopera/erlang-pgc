%% @private
-module(pgc_codec_char).

-behaviour(pgc_codec).
-export([
    init/1,
    encode/2,
    decode/2
]).


init(_Options) ->
    Info = #{
        encodes => [charsend],
        decodes => [charrecv]
    },
    {Info, []}.


encode(Char, _Options) when is_integer(Char), Char >= 0, Char < 256 ->
    [Char];
encode(Term, Options) ->
    erlang:error(badarg, [Term, Options]).


decode(<<Char>>, _Options) ->
    Char.