-module(pgc_client_codec_int4).
-moduledoc false.

-behaviour(pgc_client_codec).
-export([
    names/0,
    encode/3,
    decode/3
]).

names() ->
    [~"int4send", ~"int4recv"].

encode(Value, _TypeDescriptor, _Types) when is_integer(Value), Value >= -2147483648, Value =< 2147483647 ->
    <<Value:32/signed-integer>>;
encode(Value, TypeDescriptor, Types) ->
    erlang:error(badarg, [Value, TypeDescriptor, Types]).

decode(<<Value:32/signed-integer>>, _TypeDescriptor, _Types) ->
    Value.
