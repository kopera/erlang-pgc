-module(pgsql_codec_float8).
-behaviour(pgsql_codec).
-export([
    encodes/1,
    encode/4,
    decodes/1,
    decode/4
]).

encodes(_Opts) ->
    [<<"float8send">>].

encode(_Type, 'NaN', _Codec, _Opts) ->
    <<127, 248, 0, 0, 0, 0, 0, 0>>;
encode(_Type, infinity, _Codec, _Opts) ->
    <<127, 240, 0, 0, 0, 0, 0, 0>>;
encode(_Type, '-infinity', _Codec, _Opts) ->
    <<255, 240, 0, 0, 0, 0, 0, 0>>;
encode(_Type, Value, _Codec, _Opts) when is_number(Value) ->
    <<Value:64/signed-float>>;
encode(_Type, Value, _Codec, _Opts) ->
    error(badarg, [Value]).

decodes(_Opts) ->
    [<<"float8recv">>].

decode(_Type, <<127, 248, 0, 0, 0, 0, 0, 0>>, _Codec, _Opts) ->
    'NaN';
decode(_Type, <<127, 240, 0, 0, 0, 0, 0, 0>>, _Codec, _Opts) ->
    infinity;
decode(_Type, <<255, 240, 0, 0, 0, 0, 0, 0>>, _Codec, _Opts) ->
    '-infinity';
decode(_Type, <<Value:64/signed-float>>, _Codec, _Opts) ->
    Value.

