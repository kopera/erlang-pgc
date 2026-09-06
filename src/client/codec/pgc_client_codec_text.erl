-module(pgc_client_codec_text).
-moduledoc false.

-behaviour(pgc_client_codec).
-export([
    names/0,
    encode/3,
    decode/3
]).

names() ->
    [~"textsend", ~"textrecv", ~"varcharsend", ~"varcharrecv", ~"bpcharsend", ~"bpcharrecv", ~"citextsend", ~"citextrecv"].

encode(Value, _TypeDescriptor, _Types) ->
    _ = iolist_size(Value),
    Value.

decode(Data, _TypeDescriptor, _Types) ->
    Data.
