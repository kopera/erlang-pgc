-module(pgc_codec_text).

-behaviour(pgc_codec).
-export([
    info/1,
    encode/3,
    decode/3
]).

-export([
    parse/1
]).


info(_Options) ->
    #{
        encodes => [bpcharsend, textsend, varcharsend, charsend, namesend, citextsend],
        decodes => [bpcharrecv, textrecv, varcharrecv, charrecv, namerecv, citextrecv]
    }.


encode(_Type, Bytes, _Options) ->
    unicode:characters_to_binary(Bytes, utf8).


decode(_Type, Bytes, _Options) ->
    Bytes.


%% @private
%% Used internally in pgc_types
parse(Bytes) ->
    Bytes.
