-module(pgsql_codec_time).

-behaviour(pgsql_codec).
-export([
    init/2,
    encodes/1,
    encode/4,
    decodes/1,
    decode/4
]).

-include("../../include/types.hrl").

init(_, Opts) ->
    maps:get(time_format, Opts, record).

encodes(_Opts) ->
    [time_send, timetz_send].

encode(#pgsql_type_info{send = time_send}, Time, _Codec, Format) ->
    {H, M, S, Ms} = input(Format, Time),
    Value = (calendar:time_to_seconds({H, M, S}) * 1000000) + Ms,
    <<Value:64/signed-integer>>;
encode(#pgsql_type_info{send = timetz_send}, Time, _, Format) ->
    {H, M, S, Ms} = input(Format, Time),
    Value = (calendar:time_to_seconds({H, M, S}) * 1000000) + Ms,
    <<Value:64/signed-integer, 0:32/signed-integer>>.

decodes(_Opts) ->
    [time_recv, timetz_recv].

decode(_Type, <<Value:64/signed-integer, Timezone:32/signed-integer>>, _Codec, Format) ->
    decode_time(Format, Value + Timezone * 1000000);
decode(_Type, <<Value:64/signed-integer>>, _Codec, Format) ->
    decode_time(Format, Value);
decode(_Type, Value, _Codec, _Opts) ->
    error(badarg, [Value]).


%% Internals

decode_time(Format, Value) ->
    Ms = Value rem 1000000,
    {H, M, S} = calendar:seconds_to_time(Value div 1000000),
    output(Format, H, M, S, Ms).

input(record, #pgsql_time{hour = H, minute = M, second = S, microsecond = Ms}) ->
    {H, M, S, Ms};
input(map, #{hour := H, minute := M, second := S} = Time) ->
    Ms = maps:get(microsecond, Time, 0),
    {H, M, S, Ms};
input(calendar, {H, M, S}) ->
    {H, M, S, 0};
input(_, Time) ->
    error(badarg, [Time]).

output(record, H, M, S, Ms) ->
    #pgsql_time{hour = H, minute = M, second = S, microsecond = Ms};
output(map, H, M, S, Ms) ->
    #{hour => H, minute => M, second => S, microsecond => Ms};
output(calendar, H, M, S, _Ms) ->
    {H, M, S}.
