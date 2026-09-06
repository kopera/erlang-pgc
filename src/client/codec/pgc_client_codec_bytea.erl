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

encode(Value, _TypeDescriptor, _Types) ->
    _ = iolist_size(Value),
    Value.

decode(Data, _TypeDescriptor, _Types) when is_binary(Data) ->
    Data.
