%% @private
-module(pgc_codec_time).

-behaviour(pgc_codec).
-export([
    init/1,
    encode/2,
    decode/2
]).


init(Options) ->
    Codec = case Options of
        #{timestamp := {calendar, time}} -> {calendar, time};
        #{timestamp := Unit}
            when Unit =:= second
              ; Unit =:= millisecond
              ; Unit =:= microsecond
              ; Unit =:= nanosecond
              ; Unit =:= native -> Unit;
        #{timestamp := _} ->
            erlang:error(badarg, [Options]);
        #{} ->
            native
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
    % Microseconds = Time rem 1000000,
    % {Hours, Minutes, Seconds} = calendar:seconds_to_time(Time div 1000000),
    % to_term({Hours, Minutes, Seconds, Microseconds}).
    to_term(Time, Codec).


% ------------------------------------------------------------------------------
% Encoding
% ------------------------------------------------------------------------------

%% Internals
from_term(Term, {calendar, time}) ->
    Time = calendar:time_to_seconds(Term),
    erlang:convert_time_unit(Time, second, microsecond);
from_term(Term, Unit) ->
    erlang:convert_time_unit(Term, Unit, microsecond).


% ------------------------------------------------------------------------------
% Decoding
% ------------------------------------------------------------------------------

%% Internals
to_term(MicroSeconds, {calendar, time}) ->
    Seconds = erlang:convert_time_unit(MicroSeconds, microsecond, second),
    calendar:seconds_to_time(Seconds);
to_term(MicroSeconds, Unit) ->
    erlang:convert_time_unit(MicroSeconds, microsecond, Unit).