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

encode('NaN', _TypeDescriptor, _Types) ->
    <<127, 192, 0, 0>>;
encode(infinity, _TypeDescriptor, _Types) ->
    <<127, 128, 0, 0>>;
encode('-infinity', _TypeDescriptor, _Types) ->
    <<255, 128, 0, 0>>;
encode(Value, _TypeDescriptor, _Types) when is_number(Value) ->
    <<Value:32/signed-float>>;
encode(Value, TypeDescriptor, Types) ->
    erlang:error(badarg, [Value, TypeDescriptor, Types]).

decode(<<127, 192, 0, 0>>, _TypeDescriptor, _Types) ->
    'NaN';
decode(<<127, 128, 0, 0>>, _TypeDescriptor, _Types) ->
    infinity;
decode(<<255, 128, 0, 0>>, _TypeDescriptor, _Types) ->
    '-infinity';
decode(<<Value:32/signed-float>>, _TypeDescriptor, _Types) ->
    Value.
