-module(pgsql_codec_time).

-behaviour(pgsql_codec).
-export([
    encodes/1,
    encode/4,
    decodes/1,
    decode/4
]).

-include("../../include/types.hrl").

encodes(_Opts) ->
    [<<"time_send">>, <<"timetz_send">>].

encode(#pgsql_type_info{send = <<"time_send">>}, #pgsql_time{hours = H, minutes = M, seconds = S, micro_seconds = Ms}, _Codec, _Opts) ->
    Time = {H, M, S},
    Value = (calendar:time_to_seconds(Time) * 1000000) + Ms,
    <<Value:64/signed-integer>>;
encode(#pgsql_type_info{send = <<"timetz_send">>}, #pgsql_time{hours = H, minutes = M, seconds = S, micro_seconds = Ms}, _, _) ->
    Time = {H, M, S},
    Value = (calendar:time_to_seconds(Time) * 1000000) + Ms,
    <<Value:64/signed-integer, 0:32/signed-integer>>;
encode(_Type, Value, _Codec, _Opts) ->
    error(badarg, [Value]).

decodes(_Opts) ->
    [<<"time_recv">>, <<"timetz_recv">>].

decode(_Type, <<Value:64/signed-integer, Timezone:32/signed-integer>>, _Codec, _Opts) ->
    decode_time(Value + Timezone * 1000000);
decode(_Type, <<Value:64/signed-integer>>, _Codec, _Opts) ->
    decode_time(Value);
decode(_Type, Value, _Codec, _Opts) ->
    error(badarg, [Value]).


%% Internals

decode_time(Value) ->
    Ms = Value rem 1000000,
    {H, M, S} = calendar:seconds_to_time(Value div 1000000),
    #pgsql_time{hours = H, minutes = M, seconds = S, micro_seconds = Ms}.
