-module(pgc_client_codec_uuid).
-moduledoc false.

-behaviour(pgc_client_codec).
-export([
    names/0,
    encode/3,
    decode/3
]).

names() ->
    [~"uuid_send", ~"uuid_recv"].

encode(<<_:128>> = Uuid, _TypeDescriptor, _Types) ->
    Uuid;
encode(Value, TypeDescriptor, Types) ->
    erlang:error(badarg, [Value, TypeDescriptor, Types]).

decode(<<_:128>> = Uuid, _TypeDescriptor, _Types) ->
    Uuid.
