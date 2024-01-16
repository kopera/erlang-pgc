%% @private
-module(pgc_codec_date).

-behaviour(pgc_codec).
-export([
    init/1,
    encode/2,
    decode/2
]).

-define(epoch, 730485). % calendar:date_to_gregorian_days({2000, 1, 1})).


init(_Options) ->
    Info = #{
        encodes => [date_send],
        decodes => [date_recv]
    },
    {Info, []}.


encode(Term, Options) ->
    Date = from_term(Term),
    case calendar:valid_date(Date) of
        true ->
            Days = calendar:date_to_gregorian_days(Date) - ?epoch,
            <<Days:32/signed-integer>>;
        false ->
            error(badarg, [Term, Options])
    end.


decode(<<Days:32/signed-integer>>, _Options) ->
    Date = calendar:gregorian_days_to_date(Days + ?epoch),
    to_term(Date).


% ------------------------------------------------------------------------------
% Encoding
% ------------------------------------------------------------------------------

%% Internals
from_term({Year, Month, Day} = Date) when is_integer(Year), is_integer(Month), is_integer(Day) ->
    Date;
from_term(Term) ->
    error(badarg, [Term]).


% ------------------------------------------------------------------------------
% Decoding
% ------------------------------------------------------------------------------

%% Internals
to_term({_Year, _Month, _Day} = Date) ->
    Date.
