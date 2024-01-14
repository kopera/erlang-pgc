-module(pgc_codec_bool).

-behaviour(pgc_codec).
-export([
    init/1,
    encode/2,
    decode/2
]).


init(_Options) ->
    Info = #{
        encodes => [boolsend],
        decodes => [boolrecv]
    },
    {Info, []}.


encode(true, _Options) ->
    <<1>>;
encode(false, _Options) ->
    <<0>>;
encode(Term, Options) ->
    error(badarg, [Term, Options]).


decode(<<1>>, _Options) ->
    true;
decode(<<0>>, _Options) ->
    false.