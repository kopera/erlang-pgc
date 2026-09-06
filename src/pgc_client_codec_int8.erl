-module(pgc_client_codec_int8).
-moduledoc false.

-behaviour(pgc_client_codec).
-export([
    names/0,
    encode/3,
    decode/3
]).

names() ->
    [~"int8send", ~"int8recv"].

encode(Value, _TypeDescriptor, _Types) when is_integer(Value), Value >= -9223372036854775808, Value =< 9223372036854775807 ->
    <<Value:64/signed-integer>>;
encode(Value, TypeDescriptor, Types) ->
    erlang:error(badarg, [Value, TypeDescriptor, Types]).

decode(<<Value:64/signed-integer>>, _TypeDescriptor, _Types) ->
    Value.
