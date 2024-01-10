-module(pgc_codec_json).

-behaviour(pgc_codec).
-export([
    info/1,
    encode/3,
    decode/3
]).


-include("../pgc_type.hrl").


info(_Options) ->
    #{
        encodes => [json_send, jsonb_send],
        decodes => [json_recv, jsonb_recv]
    }.


encode(#pgc_type{send = json_send}, Value, _Options) ->
    iolist_to_binary(Value);
encode(#pgc_type{send = jsonb_send}, Value, _Options) ->
    [1, iolist_to_binary(Value)].


decode(#pgc_type{recv = json_recv}, Data, _Options) ->
    Data;
decode(#pgc_type{recv = jsonb_recv}, <<1, Data/binary>>, _Options) ->
    Data.

