-module(pgc_codec_bytea).

-behaviour(pgc_codec).
-export([
    info/1,
    encode/3,
    decode/3
]).


info(_Options) ->
    #{
        encodes => [byteasend, unknownsend],
        decodes => [bytearecv, unknownrecv]
    }.


encode(_Type, Bytes, _Options) ->
    _ = iolist_size(Bytes),
    Bytes.


decode(_Type, Bytes, _Options) when is_binary(Bytes) ->
    Bytes.

