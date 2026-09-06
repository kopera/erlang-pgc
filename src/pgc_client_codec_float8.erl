-module(pgc_client_codec_float8).
-moduledoc false.

-behaviour(pgc_client_codec).
-export([
    names/0,
    encode/3,
    decode/3
]).

names() ->
    [~"float8send", ~"float8recv"].

encode('NaN', _TypeDescriptor, _Types) ->
    <<127, 248, 0, 0, 0, 0, 0, 0>>;
encode(infinity, _TypeDescriptor, _Types) ->
    <<127, 240, 0, 0, 0, 0, 0, 0>>;
encode('-infinity', _TypeDescriptor, _Types) ->
    <<255, 240, 0, 0, 0, 0, 0, 0>>;
encode(Value, _TypeDescriptor, _Types) when is_number(Value) ->
    <<Value:64/signed-float>>;
encode(Value, TypeDescriptor, Types) ->
    erlang:error(badarg, [Value, TypeDescriptor, Types]).

decode(<<127, 248, 0, 0, 0, 0, 0, 0>>, _TypeDescriptor, _Types) ->
    'NaN';
decode(<<127, 240, 0, 0, 0, 0, 0, 0>>, _TypeDescriptor, _Types) ->
    infinity;
decode(<<255, 240, 0, 0, 0, 0, 0, 0>>, _TypeDescriptor, _Types) ->
    '-infinity';
decode(<<Value:64/signed-float>>, _TypeDescriptor, _Types) ->
    Value.
