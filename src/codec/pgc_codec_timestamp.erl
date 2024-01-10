-module(pgc_codec_timestamp).

-behaviour(pgc_codec).
-export([
    info/1,
    encode/3,
    decode/3
]).

-define(epoch, 63113904000 * 1000000). % calendar:datetime_to_gregorian_seconds({{2000, 1, 1}, {0, 0, 0}}).


info(_Options) ->
    #{
        encodes => [timestamp_send, timestamptz_send],
        decodes => [timestamp_recv, timestamptz_recv]
    }.


encode(_Type, infinity, _Options) ->
    <<16#7FFFFFFFFFFFFFFF:64/signed-integer>>;
encode(_Type, '-infinity', _Options) ->
    <<-16#8000000000000000:64/signed-integer>>;
encode(_Type, Datetime, _Options) ->
    {Year, Month, Day, Hours, Minutes, Seconds, MicroSeconds} = input(calendar, Datetime),
    Timestamp = (calendar:datetime_to_gregorian_seconds({{Year, Month, Day}, {Hours, Minutes, Seconds}})) * 1000000 + MicroSeconds - ?epoch,
    <<Timestamp:64/signed-integer>>.


decode(_Type, <<16#7FFFFFFFFFFFFFFF:64/signed-integer>>, _Options) ->
    infinity;
decode(_Type, <<-16#8000000000000000:64/signed-integer>>, _Options) ->
    '-infinity';
decode(_Type, <<Value:64/signed-integer>>, Options) ->
    MicroSeconds = Value rem 1000000,
    {{Year, Month, Day}, {Hours, Minutes, Seconds}} = calendar:gregorian_seconds_to_datetime(Value div 1000000),
    output(Options, {Year, Month, Day, Hours, Minutes, Seconds, MicroSeconds}).


%% Internals
input(_Options, {{Year, Month, Day}, {Hours, Minutes, Seconds}}) when is_integer(Seconds) ->
    {Year, Month, Day, Hours, Minutes, Seconds, 0};
input(_Options, {{Year, Month, Day}, {Hours, Minutes, SecondsFloat}}) when is_float(SecondsFloat) ->
    Seconds = trunc(SecondsFloat),
    MicroSeconds = trunc((SecondsFloat - Seconds) * 1000000),
    {Year, Month, Day, Hours, Minutes, Seconds, MicroSeconds};
input(Options, Timestamp) ->
    error(badarg, [Options, Timestamp]).


%% Internals
output(_Options, {Year, Month, Day, Hours, Minutes, Seconds, MicroSeconds}) ->
    SecondsFloat = if
        MicroSeconds =:= 0 ->
            Seconds;
        MicroSeconds =/= 0 ->
            Seconds + (MicroSeconds / 1000000)
    end,
    {{Year, Month, Day}, {Hours, Minutes, SecondsFloat}}.
