-module(pgc_client_codec_int2).
-moduledoc false.

-behaviour(pgc_client_codec).
-export([
    names/0,
    encode/3,
    decode/3
]).

names() ->
    [~"int2send", ~"int2recv"].

encode(Value, _TypeDescriptor, _Types) when is_integer(Value), Value >= -32768, Value =< 32767 ->
    <<Value:16/signed-integer>>;
encode(Value, TypeDescriptor, Types) ->
    erlang:error(badarg, [Value, TypeDescriptor, Types]).

decode(<<Value:16/signed-integer>>, _TypeDescriptor, _Types) ->
    Value.
