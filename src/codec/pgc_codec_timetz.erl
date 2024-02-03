%% @private
-module(pgc_codec_timetz).
-compile({inline, [
    from_term/2,
    to_term/2
]}).

-behaviour(pgc_codec).
-export([
    init/1,
    encode/2,
    decode/2
]).


init(Options) ->
    Codec = case Options of
        #{time := {calendar, time}} ->
            {calendar, time};
        #{time := {system_time, Unit}}
            when Unit =:= second
              ; Unit =:= millisecond
              ; Unit =:= microsecond
              ; Unit =:= nanosecond
              ; Unit =:= native ->
            {system_time, Unit};
        #{time := _} ->
            erlang:error(badarg, [Options]);
        #{} ->
            {system_time, native}
    end,
    Info = #{
        encodes => [timetz_send],
        decodes => [timetz_recv]
    },
    {Info, Codec}.


encode(Term, Codec) ->
    {Time, Offset} = from_term(Term, Codec),
    <<Time:64/signed-integer, Offset:32/signed-integer>>.


decode(<<Time:64/signed-integer, Offset:32/signed-integer>>, Codec) ->
    to_term({Time, Offset}, Codec).


% ------------------------------------------------------------------------------
% Encoding
% ------------------------------------------------------------------------------

%% Internals
from_term(Term, {calendar, time}) ->
    Seconds = calendar:time_to_seconds(Term),
    MicroSeconds = erlang:convert_time_unit(Seconds, second, microsecond),
    Offset = 0,
    {MicroSeconds, Offset};
from_term(Term, {system_time, Unit}) ->
    erlang:convert_time_unit(Term, Unit, microsecond).


% ------------------------------------------------------------------------------
% Decoding
% ------------------------------------------------------------------------------

%% Internals
to_term({MicroSeconds, Offset}, {calendar, time}) ->
    Seconds = erlang:convert_time_unit(MicroSeconds, microsecond, second) + Offset,
    calendar:seconds_to_time(Seconds);
to_term({MicroSeconds, Offset}, {system_time, Unit}) ->
    erlang:convert_time_unit(MicroSeconds, microsecond, Unit) +
    erlang:convert_time_unit(Offset, second, Unit).
