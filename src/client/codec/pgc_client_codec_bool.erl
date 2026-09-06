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

encode(true, _TypeDescriptor, _Types) ->
    <<1>>;
encode(false, _TypeDescriptor, _Types) ->
    <<0>>;
encode(Value, TypeDescriptor, Types) ->
    erlang:error(badarg, [Value, TypeDescriptor, Types]).

decode(<<1>>, _TypeDescriptor, _Types) ->
    true;
decode(<<0>>, _TypeDescriptor, _Types) ->
    false.
