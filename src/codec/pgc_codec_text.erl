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

-include("../pgc_type.hrl").


info(_Options) ->
    #{
        encodes => [bpcharsend, textsend, varcharsend, charsend, namesend, citextsend],
        decodes => [bpcharrecv, textrecv, varcharrecv, charrecv, namerecv, citextrecv]
    }.


encode(#pgc_type{send = namesend}, String, _Options) when is_binary(String), byte_size(String) < 64 ->
    String;
encode(#pgc_type{send = namesend} = Type, String, Options) when is_list(String) ->
    case erlang:iolist_size(String) of
        Length when Length < 64 -> String;
        Length when Length >= 64 -> erlang:error(badarg, [Type, String, Options])
    end;
encode(_Type, String, _Options) ->
    unicode:characters_to_binary(String, utf8).


decode(_Type, Bytes, _Options) ->
    Bytes.


%% @private
%% Used internally in pgc_types
parse(Bytes) ->
    Bytes.
