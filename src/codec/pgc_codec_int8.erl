-module(pgc_codec_int8).

-behaviour(pgc_codec).
-export([
    info/1,
    encode/3,
    decode/3
]).


info(_Options) ->
    #{
        encodes => [int8send],
        decodes => [int8recv]
    }.


encode(_Type, Value, _Options) when is_integer(Value), Value >= -9223372036854775808, Value =< 9223372036854775807 ->
    <<Value:64/signed-integer>>;
encode(Type, Value, Options) ->
    error(badarg, [Type, Value, Options]).


decode(_Type, <<Value:64/signed-integer>>, _Options) ->
    Value.
