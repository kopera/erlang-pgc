-module(pgsql_codec_datetime).
-behaviour(pgsql_codec).
-export([
    init/2,
    encodes/1,
    encode/4,
    decodes/1,
    decode/4
]).

-include("../../include/types.hrl").
-define(epoch, 63113904000 * 1000000). % calendar:datetime_to_gregorian_seconds({{2000, 1, 1}, {0, 0, 0}}).

init(_, Opts) ->
    maps:get(datetime_format, Opts, record).

encodes(_Opts) ->
    [timestamp_send, timestamptz_send].

encode(_Type, Datetime, _Codec, Format) ->
    {Y, M, D, Hr, Mn, Sc, Ms} = input(Format, Datetime),
    Timestamp = (calendar:datetime_to_gregorian_seconds({{Y, M, D}, {Hr, Mn, Sc}})) * 1000000 + Ms - ?epoch,
    <<Timestamp:64/signed-integer>>.

decodes(_Opts) ->
    [timestamp_recv, timestamptz_recv].

decode(_Type, <<Value:64/signed-integer>>, _Codec, Format) ->
    decode_datetime(Format, Value + ?epoch).


%% Internals

decode_datetime(Format, Value) ->
    Ms = Value rem 1000000,
    {{Y, M, D}, {Hr, Mn, Sc}} = calendar:gregorian_seconds_to_datetime((Value div 1000000)),
    output(Format, Y, M, D, Hr, Mn, Sc, Ms).

input(record, #pgsql_datetime{year = Y, month = M, day = D, hours = Hr, minutes = Mn, seconds = Sc, micro_seconds = Ms}) ->
    {Y, M, D, Hr, Mn, Sc, Ms};
input(map, #{year := Y, month := M, day := D, hours := Hr, minutes := Mn, seconds := Sc} = Datetime) ->
    Ms = maps:get(micro_seconds, Datetime, 0),
    {Y, M, D, Hr, Mn, Sc, Ms};
input(calendar, {{Y, M, D}, {Hr, Mn, Sc}}) ->
    {Y, M, D, Hr, Mn, Sc, 0};
input(_, Time) ->
    error(badarg, [Time]).

output(record, Y, M, D, Hr, Mn, Sc, Ms) ->
    #pgsql_datetime{year = Y, month = M, day = D, hours = Hr, minutes = Mn, seconds = Sc, micro_seconds = Ms};
output(map, Y, M, D, Hr, Mn, Sc, Ms) ->
    #{year => Y, month => M, day => D, hours => Hr, minutes => Mn, seconds => Sc, micro_seconds => Ms};
output(calendar, Y, M, D, Hr, Mn, Sc, _Ms) ->
    {{Y, M, D}, {Hr, Mn, Sc}}.
