-module(pgsql_codec_uuid).
-behaviour(pgsql_codec).
-export([
    encodes/1,
    encode/4,
    decodes/1,
    decode/4
]).

encodes(_Opts) ->
    [uuid_send].

encode(_Type, <<_:128>> = UUID, _Codec, _Opts) ->
    UUID;
encode(_Type, Value, _Codec, _Opts) ->
    error(badarg, [Value]).

decodes(_Opts) ->
    [uuid_recv].

decode(_Type, <<_:128>> = UUID, _Codec, _Opts) ->
    UUID.

