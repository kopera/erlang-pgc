-module(pgc_client_codec_char).
-moduledoc false.

-behaviour(pgc_client_codec).
-export([
    names/0,
    encode/3,
    decode/3
]).

names() ->
    [~"charsend", ~"charrecv"].

encode(Char, _TypeDescriptor, _Codecs) when is_integer(Char), Char >= 0, Char < 256 ->
    <<Char>>;
encode(Value, TypeDescriptor, Codecs) ->
    erlang:error(badarg, [Value, TypeDescriptor, Codecs]).

decode(<<Char>>, _TypeDescriptor, _Codecs) ->
    Char.
