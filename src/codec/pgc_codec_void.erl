-module(pgc_codec_void).

-behaviour(pgc_codec).
-export([
    info/1,
    encode/3,
    decode/3
]).


info(_Options) ->
    #{
        encodes => [void_send],
        decodes => [void_recv]
    }.


encode(_Type, undefined, _Options) ->
    <<>>;
encode(Type, Value, Options) ->
    error(badarg, [Type, Value, Options]).


decode(_Type, <<>>, _Options) ->
    undefined.
