-module(pgc_client_codec_jsonb).
-moduledoc false.

-behaviour(pgc_client_codec).
-export([
    names/0,
    encode/3,
    decode/3
]).

names() ->
    [~"jsonb_send", ~"jsonb_recv"].

-doc "Same `codecs => #{json => #{codec => Module}}` option as [`pgc_client_codec_json`](`m:pgc_client_codec_json`) -- jsonb only adds a leading version byte on the wire.".
encode(Term, TypeDescriptor, Codecs) ->
    [1 | pgc_client_codec_json:encode(Term, TypeDescriptor, Codecs)].

decode(<<1, Data/binary>>, TypeDescriptor, Codecs) ->
    pgc_client_codec_json:decode(Data, TypeDescriptor, Codecs).
