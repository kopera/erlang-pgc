-module(pgc_client_codec_xid8).
-moduledoc false.

-behaviour(pgc_client_codec).
-export([
    names/0,
    encode/3,
    decode/3
]).

names() ->
    [~"xid8send", ~"xid8recv"].

encode(Value, _TypeDescriptor, _Codecs) when is_integer(Value), Value >= 0, Value =< 18_446_744_073_709_551_615 ->
    <<Value:64/integer>>;
encode(Value, TypeDescriptor, Codecs) ->
    erlang:error(badarg, [Value, TypeDescriptor, Codecs]).

decode(<<Value:64/integer>>, _TypeDescriptor, _Codecs) ->
    Value.
