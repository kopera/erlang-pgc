-module(pgc_codec_date).

-behaviour(pgc_codec).
-export([
    info/1,
    encode/3,
    decode/3
]).

-define(epoch, 730485). % calendar:date_to_gregorian_days({2000, 1, 1})).


info(_Options) ->
    #{
        encodes => [date_send],
        decodes => [date_recv]
    }.


encode(Type, Term, Options) ->
    Date = from_term(Options, Term),
    case calendar:valid_date(Date) of
        true ->
            Days = calendar:date_to_gregorian_days(Date) - ?epoch,
            <<Days:32/signed-integer>>;
        false ->
            error(badarg, [Type, Term, Options])
    end.


decode(_Type, <<Days:32/signed-integer>>, Options) ->
    Date = calendar:gregorian_days_to_date(Days + ?epoch),
    to_term(Options, Date).


% ------------------------------------------------------------------------------
% Encoding
% ------------------------------------------------------------------------------

%% Internals
from_term(_Options, {Year, Month, Day} = Date) when is_integer(Year), is_integer(Month), is_integer(Day) ->
    Date;
from_term(Options, Term) ->
    error(badarg, [Options, Term]).


% ------------------------------------------------------------------------------
% Decoding
% ------------------------------------------------------------------------------

%% Internals
to_term(_Options, {_Year, _Month, _Day} = Date) ->
    Date.
