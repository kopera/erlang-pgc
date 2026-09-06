-module(pgc_client_codec_ltree).
-moduledoc false.

-behaviour(pgc_client_codec).
-export([
    names/0,
    encode/3,
    decode/3
]).

names() ->
    [~"ltree_send", ~"ltree_recv", ~"lquery_send", ~"lquery_recv"].

encode(Value, _TypeDescriptor, _Codecs) when is_binary(Value) ->
    <<1:8, Value/binary>>;
encode(Value, TypeDescriptor, Codecs) ->
    erlang:error(badarg, [Value, TypeDescriptor, Codecs]).

decode(<<1:8, Value/binary>>, _TypeDescriptor, _Codecs) ->
    Value.
