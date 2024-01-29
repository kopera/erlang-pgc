%% @private
-module(pgc_codec_int8).

-behaviour(pgc_codec).
-export([
    init/1,
    encode/2,
    decode/2
]).


init(_Options) ->
    Info = #{
        encodes => [int8send],
        decodes => [int8recv]
    },
    {Info, []}.


encode(Value, _Options) when is_integer(Value), Value >= -9223372036854775808, Value =< 9223372036854775807 ->
    <<Value:64/signed-integer>>;
encode(Value, Options) ->
    error(badarg, [Value, Options]).


decode(<<Value:64/signed-integer>>, _Options) ->
    Value.
