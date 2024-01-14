-module(pgc_codec_float4).

-behaviour(pgc_codec).
-export([
    init/1,
    encode/2,
    decode/2
]).


init(_Options) ->
    Info = #{
        encodes => [float4send],
        decodes => [float4recv]
    },
    {Info, []}.


encode('NaN', _Options) ->
    <<127, 192, 0, 0>>;
encode(infinity, _Options) ->
    <<127, 128, 0, 0>>;
encode('-infinity', _Options) ->
    <<255, 128, 0, 0>>;
encode(Value, _Options) when is_number(Value) ->
    <<Value:32/signed-float>>;
encode(Value, Options) ->
    error(badarg, [Value, Options]).


decode(<<127, 192, 0, 0>>, _Options) ->
    'NaN';
decode(<<127, 128, 0, 0>>, _Options) ->
    infinity;
decode(<<255, 128, 0, 0>>, _Options) ->
    '-infinity';
decode(<<Value:32/signed-float>>, _Options) ->
    Value.
