%% @private
-module(pgc_codec_int2).

-behaviour(pgc_codec).
-export([
    init/1,
    encode/2,
    decode/2
]).


init(_Options) ->
    Info = #{
        encodes => [int2send],
        decodes => [int2recv]
    },
    {Info, []}.


encode(Value, _Options) when is_integer(Value), Value >= -32768, Value =< 32767 ->
    <<Value:16/signed-integer>>;
encode(Value, Options) ->
    error(badarg, [Value, Options]).


decode(<<Value:16/signed-integer>>, _Options) ->
    Value.