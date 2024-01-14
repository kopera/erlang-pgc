-module(pgc_codec_uuid).

-behaviour(pgc_codec).
-export([
    init/1,
    encode/2,
    decode/2
]).


init(_Options) ->
    Info = #{
        encodes => [uuid_send],
        decodes => [uuid_recv]
    },
    {Info, []}.


encode(<<_:128>> = UUID, _Options) ->
    UUID;
encode(Value, Options) ->
    error(badarg, [Value, Options]).


decode(<<_:128>> = UUID, _Options) ->
    UUID.