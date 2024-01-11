-module(pgc_codec_time).

-behaviour(pgc_codec).
-export([
    info/1,
    encode/3,
    decode/3
]).


info(_Options) ->
    #{
        encodes => [time_send],
        decodes => [time_recv]
    }.


encode(_Type, Term, Options) ->
    {Hours, Minutes, Seconds, Microseconds} = from_term(Options, Term),
    Value = (calendar:time_to_seconds({Hours, Minutes, Seconds}) * 1000000) + Microseconds,
    <<Value:64/signed-integer>>.


decode(_Type, <<Time:64/signed-integer>>, Options) ->
    Microseconds = Time rem 1000000,
    {Hours, Minutes, Seconds} = calendar:seconds_to_time(Time div 1000000),
    to_term(Options, {Hours, Minutes, Seconds, Microseconds}).


% ------------------------------------------------------------------------------
% Encoding
% ------------------------------------------------------------------------------

%% Internals
from_term(_Options, {Hours, Minutes, Seconds}) when is_integer(Hours), is_integer(Minutes), is_integer(Seconds) ->
    {Hours, Minutes, Seconds, 0};
from_term(_Options, {Hours, Minutes, SecondsFloat}) when is_integer(Hours), is_integer(Minutes), is_float(SecondsFloat) ->
    Seconds = trunc(SecondsFloat),
    MicroSeconds = trunc((SecondsFloat - Seconds) * 1000000),
    {Hours, Minutes, Seconds, MicroSeconds};
from_term(Options, Term) ->
    error(badarg, [Options, Term]).


% ------------------------------------------------------------------------------
% Decoding
% ------------------------------------------------------------------------------

%% Internals
to_term(_Options, {Hours, Minutes, Seconds, 0}) ->
    {Hours, Minutes, Seconds};
to_term(_Options, {Hours, Minutes, Seconds, MicroSeconds}) ->
    {Hours, Minutes, Seconds + (MicroSeconds / 1000000)}.