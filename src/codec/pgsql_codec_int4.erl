-module(pgsql_codec_int4).
-behaviour(pgsql_codec).
-export([
    encodes/1,
    encode/4,
    decodes/1,
    decode/4
]).

encodes(_Opts) ->
    [int4send].

encode(_Type, Value, _Codec, _Opts) when is_integer(Value), Value >= -2147483648, Value =< 2147483647 ->
    <<Value:32/signed-integer>>;
encode(_Type, Value, _Codec, _Opts) ->
    error(badarg, [Value]).

decodes(_Opts) ->
    [int4recv].

decode(_Type, <<Value:32/signed-integer>>, _Codec, _Opts) ->
    Value.

