-module(pgc_codec_uuid).

-behaviour(pgc_codec).
-export([
    info/1,
    encode/3,
    decode/3
]).


info(_Options) ->
    #{
        encodes => [uuid_send],
        decodes => [uuid_recv]
    }.


encode(_Type, <<_:128>> = UUID, _Options) ->
    UUID;
encode(Type, Value, Options) ->
    error(badarg, [Type, Value, Options]).


decode(_Type, <<_:128>> = UUID, _Options) ->
    UUID.