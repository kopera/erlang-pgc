-module(pgsql_codec_bool).

-behaviour(pgsql_codec).
-export([
    encodes/1,
    encode/4,
    decodes/1,
    decode/4
]).

encodes(_Opts) ->
    [<<"boolsend">>].

encode(_Type, true, _Codec, _Opts) ->
    <<1>>;
encode(_Type, false, _Codec, _Opts) ->
    <<0>>;
encode(_Type, Value, _Codec, _Opts) ->
    error(badarg, [Value]).

decodes(_Opts) ->
    [<<"boolrecv">>].

decode(_Type, <<1>>, _Codec, _Opts) ->
    true;
decode(_Type, <<0>>, _Codec, _Opts) ->
    false.
