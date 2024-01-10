-module(pgc_codec_float8).

-behaviour(pgc_codec).
-export([
    info/1,
    encode/3,
    decode/3
]).

info(_Options) ->
    #{
        encodes => [float8send],
        decodes => [float8recv]
    }.


encode(_Type, 'NaN', _Options) ->
    <<127, 248, 0, 0, 0, 0, 0, 0>>;
encode(_Type, infinity, _Options) ->
    <<127, 240, 0, 0, 0, 0, 0, 0>>;
encode(_Type, '-infinity', _Options) ->
    <<255, 240, 0, 0, 0, 0, 0, 0>>;
encode(_Type, Value, _Options) when is_number(Value) ->
    <<Value:64/signed-float>>;
encode(Type, Value, Options) ->
    error(badarg, [Type, Value, Options]).


decode(_Type, <<127, 248, 0, 0, 0, 0, 0, 0>>, _Options) ->
    'NaN';
decode(_Type, <<127, 240, 0, 0, 0, 0, 0, 0>>, _Options) ->
    infinity;
decode(_Type, <<255, 240, 0, 0, 0, 0, 0, 0>>, _Options) ->
    '-infinity';
decode(_Type, <<Value:64/signed-float>>, _Options) ->
    Value.

