-module(pgc_client_codec_timetz).
-moduledoc false.

-behaviour(pgc_client_codec).
-export([
    names/0,
    encode/3,
    decode/3
]).

names() ->
    [~"timetz_send", ~"timetz_recv"].

-doc "Same `codecs => #{time => #{representation => Rep}}` option as [`pgc_client_codec_time`](`m:pgc_client_codec_time`); the wire time zone offset is not represented in either term shape and is always encoded as `0`.".
encode(Term, _TypeDescriptor, Codecs) ->
    MicroSeconds = pgc_client_codec_time:from_term(Term, representation(Codecs)),
    <<MicroSeconds:64/signed-integer, 0:32/signed-integer>>.

decode(<<MicroSeconds:64/signed-integer, Offset:32/signed-integer>>, _TypeDescriptor, Codecs) ->
    pgc_client_codec_time:to_term(MicroSeconds + Offset * 1_000_000, representation(Codecs)).

representation(Codecs) ->
    maps:get(representation, pgc_client_codecs:options(time, Codecs), {system_time, native}).
