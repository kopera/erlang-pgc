-module(pgc_codec_void).

-behaviour(pgc_codec).
-export([
    init/1,
    encode/2,
    decode/2
]).


init(_Options) ->
    Info = #{
        encodes => [void_send],
        decodes => [void_recv]
    },
    {Info, []}.


encode(undefined, _Options) ->
    <<>>;
encode(Value, Options) ->
    error(badarg, [Value, Options]).


decode(<<>>, _Options) ->
    undefined.
