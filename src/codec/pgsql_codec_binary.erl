-module(pgsql_codec_binary).
-behaviour(pgsql_codec).
-export([
    encodes/1,
    encode/4,
    decodes/1,
    decode/4
]).

encodes(_Opts) ->
    [<<"byteasend">>, <<"unknownsend">>].

encode(_Type, Bytes, _Codec, _Opts) when is_list(Bytes); is_binary(Bytes) ->
    iolist_to_binary(Bytes);
encode(_Type, Value, _Codec, _Opts) ->
    error(badarg, [Value]).

decodes(_Opts) ->
    [<<"bytearecv">>, <<"unknownrecv">>].

decode(_Type, Bytes, _Codec, _Opts) ->
    binary:copy(Bytes).

