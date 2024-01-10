-module(pgc_codec_int2).

-behaviour(pgc_codec).
-export([
    info/1,
    encode/3,
    decode/3
]).


info(_Options) ->
    #{
        encodes => [int2send],
        decodes => [int2recv]
    }.


encode(_Type, Value, _Options) when is_integer(Value), Value >= -32768, Value =< 32767 ->
    <<Value:16/signed-integer>>;
encode(Type, Value, Options) ->
    error(badarg, [Type, Value, Options]).


decode(_Type, <<Value:16/signed-integer>>, _Options) ->
    Value.