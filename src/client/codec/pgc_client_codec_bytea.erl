-module(pgc_client_codec_bytea).
-moduledoc false.

-behaviour(pgc_client_codec).
-export([
    names/0,
    encode/3,
    decode/3
]).

names() ->
    [~"byteasend", ~"bytearecv", ~"unknownsend", ~"unknownrecv"].

encode(Value, _TypeDescriptor, _Codecs) ->
    _ = iolist_size(Value),
    Value.

decode(Data, _TypeDescriptor, _Codecs) when is_binary(Data) ->
    Data.
