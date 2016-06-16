-module(pgsql_codec_float4).
-behaviour(pgsql_codec).
-export([
    encodes/1,
    encode/4,
    decodes/1,
    decode/4
]).

encodes(_Opts) ->
    [<<"float4send">>].

encode(_Type, 'NaN', _Codec, _Opts) ->
    <<127, 192, 0, 0>>;
encode(_Type, infinity, _Codec, _Opts) ->
    <<127, 128, 0, 0>>;
encode(_Type, '-infinity', _Codec, _Opts) ->
    <<255, 128, 0, 0>>;
encode(_Type, Value, _Codec, _Opts) when is_number(Value) ->
    <<Value:32/signed-float>>;
encode(_Type, Value, _Codec, _Opts) ->
    error(badarg, [Value]).

decodes(_Opts) ->
    [<<"float4recv">>].

decode(_Type, <<127, 192, 0, 0>>, _Codec, _Opts) ->
    'NaN';
decode(_Type, <<127, 128, 0, 0>>, _Codec, _Opts) ->
    infinity;
decode(_Type, <<255, 128, 0, 0>>, _Codec, _Opts) ->
    '-infinity';
decode(_Type, <<Value:32/signed-float>>, _Codec, _Opts) ->
    Value.

