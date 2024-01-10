-module(pgc_codec_float4).

-behaviour(pgc_codec).
-export([
    info/1,
    encode/3,
    decode/3
]).


info(_Options) ->
    #{
        encodes => [float4send],
        decodes => [float4recv]
    }.


encode(_Type, 'NaN', _Options) ->
    <<127, 192, 0, 0>>;
encode(_Type, infinity, _Options) ->
    <<127, 128, 0, 0>>;
encode(_Type, '-infinity', _Options) ->
    <<255, 128, 0, 0>>;
encode(_Type, Value, _Options) when is_number(Value) ->
    <<Value:32/signed-float>>;
encode(Type, Value, Options) ->
    error(badarg, [Type, Value, Options]).


decode(_Type, <<127, 192, 0, 0>>, _Options) ->
    'NaN';
decode(_Type, <<127, 128, 0, 0>>, _Options) ->
    infinity;
decode(_Type, <<255, 128, 0, 0>>, _Options) ->
    '-infinity';
decode(_Type, <<Value:32/signed-float>>, _Options) ->
    Value.
