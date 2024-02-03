%% @private
-module(pgc_codec_time).
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
        encodes => [time_send],
        decodes => [time_recv]
    },
    {Info, Codec}.


encode(Term, Codec) ->
    Time = from_term(Term, Codec),
    <<Time:64/signed-integer>>.


decode(<<Time:64/signed-integer>>, Codec) ->
    to_term(Time, Codec).


% ------------------------------------------------------------------------------
% Encoding
% ------------------------------------------------------------------------------

%% Internals
from_term(Term, {calendar, time}) ->
    Time = calendar:time_to_seconds(Term),
    erlang:convert_time_unit(Time, second, microsecond);
from_term(Term, {system_time, Unit}) ->
    erlang:convert_time_unit(Term, Unit, microsecond).


% ------------------------------------------------------------------------------
% Decoding
% ------------------------------------------------------------------------------

%% Internals
to_term(MicroSeconds, {calendar, time}) ->
    Seconds = erlang:convert_time_unit(MicroSeconds, microsecond, second),
    calendar:seconds_to_time(Seconds);
to_term(MicroSeconds, {system_time, Unit}) ->
    erlang:convert_time_unit(MicroSeconds, microsecond, Unit).
