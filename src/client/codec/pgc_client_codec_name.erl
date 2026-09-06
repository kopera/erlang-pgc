-module(pgc_client_codec_name).
-moduledoc false.

-behaviour(pgc_client_codec).
-export([
    names/0,
    encode/3,
    decode/3
]).

names() ->
    [~"namesend", ~"namerecv"].

encode(Value, _TypeDescriptor, _Codecs) when is_binary(Value), byte_size(Value) < 64 ->
    Value;
encode(Value, TypeDescriptor, Codecs) ->
    erlang:error(badarg, [Value, TypeDescriptor, Codecs]).

decode(Data, _TypeDescriptor, _Codecs) ->
    Data.
