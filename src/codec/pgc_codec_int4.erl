-module(pgc_codec_int4).

-behaviour(pgc_codec).
-export([
    info/1,
    encode/3,
    decode/3
]).


info(_Options) ->
    #{
        encodes => [int4send],
        decodes => [int4recv]
    }.


encode(_Type, Value, _Options) when is_integer(Value), Value >= -2147483648, Value =< 2147483647 ->
    <<Value:32/signed-integer>>;
encode(Type, Value, Options) ->
    error(badarg, [Type, Value, Options]).


decode(_Type, <<Value:32/signed-integer>>, _Options) ->
    Value.