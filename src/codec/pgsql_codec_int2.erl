-module(pgsql_codec_int2).
-behaviour(pgsql_codec).
-export([
    encodes/1,
    encode/4,
    decodes/1,
    decode/4
]).

encodes(_Opts) ->
    [int2send].

encode(_Type, Value, _Codec, _Opts) when is_integer(Value), Value >= -32768, Value =< 32767 ->
    <<Value:16/signed-integer>>;
encode(_Type, Value, _Codec, _Opts) ->
    error(badarg, [Value]).

decodes(_Opts) ->
    [int2recv].

decode(_Type, <<Value:16/signed-integer>>, _Codec, _Opts) ->
    Value.

