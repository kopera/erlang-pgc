-module(pgc_client_codec_bool).
-moduledoc false.

-behaviour(pgc_client_codec).
-export([
    names/0,
    encode/3,
    decode/3
]).

names() ->
    [~"boolsend", ~"boolrecv"].

encode(true, _TypeDescriptor, _Codecs) ->
    <<1>>;
encode(false, _TypeDescriptor, _Codecs) ->
    <<0>>;
encode(Value, TypeDescriptor, Codecs) ->
    erlang:error(badarg, [Value, TypeDescriptor, Codecs]).

decode(<<1>>, _TypeDescriptor, _Codecs) ->
    true;
decode(<<0>>, _TypeDescriptor, _Codecs) ->
    false.
