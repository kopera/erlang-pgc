-module(pgsql_codec_json).
-behaviour(pgsql_codec).
-export([
    encodes/1,
    encode/4,
    decodes/1,
    decode/4
]).

-include("../../include/types.hrl").

encodes(_Opts) ->
    [json_send, jsonb_send].

encode(#pgsql_type_info{send = json_send}, Value, _Codec, _Opts) ->
    jsone:encode(Value);
encode(#pgsql_type_info{send = jsonb_send}, Value, _Codec, _Opts) ->
    [1, jsone:encode(Value)];
encode(_Type, Value, _Codec, _Opts) ->
    error(badarg, [Value]).

decodes(_Opts) ->
    [json_recv, jsonb_recv].

decode(#pgsql_type_info{recv = json_recv}, Data, _Codec, _Opts) ->
    jsone:decode(Data);
decode(#pgsql_type_info{recv = jsonb_recv}, <<1, Data/binary>>, _Codec, _Opts) ->
    jsone:decode(Data).

