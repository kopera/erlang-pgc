-module(pgsql_codec_int8).
-behaviour(pgsql_codec).
-export([
    encodes/1,
    encode/4,
    decodes/1,
    decode/4
]).

encodes(_Opts) ->
    [<<"int8send">>].

encode(_Type, Value, _Codec, _Opts) when is_integer(Value), Value >= -9223372036854775808, Value =< 9223372036854775807 ->
    <<Value:64/signed-integer>>;
encode(_Type, Value, _Codec, _Opts) ->
    error(badarg, [Value]).

decodes(_Opts) ->
    [<<"int8recv">>].

decode(_Type, <<Value:64/signed-integer>>, _Codec, _Opts) ->
    Value.

