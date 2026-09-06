-module(pgc_client_codec_float4).
-moduledoc false.

-behaviour(pgc_client_codec).
-export([
    names/0,
    encode/3,
    decode/3
]).

names() ->
    [~"float4send", ~"float4recv"].

encode('NaN', _TypeDescriptor, _Codecs) ->
    <<127, 192, 0, 0>>;
encode(infinity, _TypeDescriptor, _Codecs) ->
    <<127, 128, 0, 0>>;
encode('-infinity', _TypeDescriptor, _Codecs) ->
    <<255, 128, 0, 0>>;
encode(Value, _TypeDescriptor, _Codecs) when is_number(Value) ->
    <<Value:32/signed-float>>;
encode(Value, TypeDescriptor, Codecs) ->
    erlang:error(badarg, [Value, TypeDescriptor, Codecs]).

decode(<<127, 192, 0, 0>>, _TypeDescriptor, _Codecs) ->
    'NaN';
decode(<<127, 128, 0, 0>>, _TypeDescriptor, _Codecs) ->
    infinity;
decode(<<255, 128, 0, 0>>, _TypeDescriptor, _Codecs) ->
    '-infinity';
decode(<<Value:32/signed-float>>, _TypeDescriptor, _Codecs) ->
    Value.
