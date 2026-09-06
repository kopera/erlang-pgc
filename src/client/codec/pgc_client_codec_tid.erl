-module(pgc_client_codec_tid).
-moduledoc false.

-behaviour(pgc_client_codec).
-export([
    names/0,
    encode/3,
    decode/3
]).

names() ->
    [~"tidsend", ~"tidrecv"].

encode({Block, Tuple}, _TypeDescriptor, _Codecs) when is_integer(Block), Block >= 0, Block =< 4294967295, is_integer(Tuple), Tuple >= 0, Tuple =< 65535 ->
    <<Block:32/integer, Tuple:16/integer>>;
encode(Value, TypeDescriptor, Codecs) ->
    erlang:error(badarg, [Value, TypeDescriptor, Codecs]).

decode(<<Block:32/integer, Tuple:16/integer>>, _TypeDescriptor, _Codecs) ->
    {Block, Tuple}.
