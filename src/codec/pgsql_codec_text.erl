-module(pgsql_codec_text).
-behaviour(pgsql_codec).
-export([
    encodes/1,
    encode/4,
    decodes/1,
    decode/4
]).

-export([
    decode/1
]).

encodes(_Opts) ->
    [<<"bpcharsend">>, <<"textsend">>, <<"varcharsend">>, <<"charsend">>, <<"namesend">>, <<"citextsend">>].

encode(_Type, Bytes, _Codec, _Opts) when is_binary(Bytes); is_list(Bytes) ->
    unicode:characters_to_binary(Bytes);
encode(_Type, Value, _Codec, _Opts) ->
    error(badarg, [Value]).

decodes(_Opts) ->
    [<<"bpcharrecv">>, <<"textrecv">>, <<"varcharrecv">>, <<"charrecv">>, <<"namerecv">>, <<"citextrecv">>].

decode(_Type, Bytes, _Codec, _Opts) ->
    binary:copy(Bytes).

%% @private
%% Used internally in pgsql_types
decode(Bytes) ->
    binary:copy(Bytes).

