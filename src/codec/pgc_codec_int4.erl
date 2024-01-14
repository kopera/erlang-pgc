-module(pgc_codec_int4).

-behaviour(pgc_codec).
-export([
    init/1,
    encode/2,
    decode/2
]).


init(_Options) ->
    Info = #{
        encodes => [int4send],
        decodes => [int4recv]
    },
    {Info, []}.


encode(Value, _Options) when is_integer(Value), Value >= -2147483648, Value =< 2147483647 ->
    <<Value:32/signed-integer>>;
encode(Value, Options) ->
    error(badarg, [Value, Options]).


decode(<<Value:32/signed-integer>>, _Options) ->
    Value.