-module(pgsql_codec_void).
-behaviour(pgsql_codec).
-export([
    encodes/1,
    encode/4,
    decodes/1,
    decode/4
]).

encodes(_Opts) ->
    [<<"void_send">>].

encode(_Type, void, _Codec, _Opts) ->
    <<>>;
encode(_Type, Value, _Codec, _Opts) ->
    error(badarg, [Value]).

decodes(_Opts) ->
    [<<"void_recv">>].

decode(_Type, <<>>, _Codec, _Opts) ->
    void.

