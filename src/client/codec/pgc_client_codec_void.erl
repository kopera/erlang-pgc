-module(pgc_client_codec_void).
-moduledoc false.

-behaviour(pgc_client_codec).
-export([
    names/0,
    encode/3,
    decode/3
]).

names() ->
    [~"void_send", ~"void_recv"].

encode(undefined, _TypeDescriptor, _Codecs) ->
    <<>>;
encode(Value, TypeDescriptor, Codecs) ->
    erlang:error(badarg, [Value, TypeDescriptor, Codecs]).

decode(<<>>, _TypeDescriptor, _Codecs) ->
    undefined.
