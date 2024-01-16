%% @private
-module(pgc_codec_float8).

-behaviour(pgc_codec).
-export([
    init/1,
    encode/2,
    decode/2
]).


init(_Options) ->
    Info = #{
        encodes => [float8send],
        decodes => [float8recv]
    },
    {Info, []}.



encode('NaN', _Options) ->
    <<127, 248, 0, 0, 0, 0, 0, 0>>;
encode(infinity, _Options) ->
    <<127, 240, 0, 0, 0, 0, 0, 0>>;
encode('-infinity', _Options) ->
    <<255, 240, 0, 0, 0, 0, 0, 0>>;
encode(Value, _Options) when is_number(Value) ->
    <<Value:64/signed-float>>;
encode(Value, Options) ->
    error(badarg, [Value, Options]).


decode(<<127, 248, 0, 0, 0, 0, 0, 0>>, _Options) ->
    'NaN';
decode(<<127, 240, 0, 0, 0, 0, 0, 0>>, _Options) ->
    infinity;
decode(<<255, 240, 0, 0, 0, 0, 0, 0>>, _Options) ->
    '-infinity';
decode(<<Value:64/signed-float>>, _Options) ->
    Value.

