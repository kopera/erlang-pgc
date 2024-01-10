-module(pgc_codec_bool).

-behaviour(pgc_codec).
-export([
    info/1,
    encode/3,
    decode/3
]).


info(_Options) ->
    #{
        encodes => [boolsend],
        decodes => [boolrecv]
    }.


encode(_Type, true, _Options) ->
    <<1>>;
encode(_Type, false, _Options) ->
    <<0>>;
encode(Type, Value, Options) ->
    error(badarg, [Type, Value, Options]).


decode(_Type, <<1>>, _Options) ->
    true;
decode(_Type, <<0>>, _Options) ->
    false.