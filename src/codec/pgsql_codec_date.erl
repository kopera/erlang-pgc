-module(pgsql_codec_date).

-behaviour(pgsql_codec).
-export([
    init/2,
    encodes/1,
    encode/4,
    decodes/1,
    decode/4
]).

-include("../../include/types.hrl").
-define(epoch, 730485). % calendar:date_to_gregorian_days({2000, 1, 1})).

init(_, Opts) ->
    maps:get(date_format, Opts, record).

encodes(_Opts) ->
    [date_send].

encode(_Type, Date, _Codec, Format) ->
    Date = input(Format, Date),
    case calendar:valid_date(Date) of
        true -> <<(calendar:date_to_gregorian_days(Date) - ?epoch):32/signed-integer>>;
        false -> error(badarg, [Date])
    end.

decodes(_Opts) ->
    [date_recv].

decode(_Type, <<Days:32/signed-integer>>, _Codec, Format) ->
    {Year, Month, Day} = calendar:gregorian_days_to_date(Days + ?epoch),
    output(Format, Year, Month, Day).


% Internals

input(record, #pgsql_date{year = Year, month = Month, day = Day}) ->
    {Year, Month, Day};
input(map, #{year := Year, month := Month, day := Day}) ->
    {Year, Month, Day};
input(calendar, {_, _, _} = Date) ->
    Date;
input(_, Date) ->
    error(badarg, [Date]).

output(record, Year, Month, Day) ->
    #pgsql_date{year = Year, month = Month, day = Day};
output(map, Year, Month, Day) ->
    #{year => Year, month => Month, day => Day};
output(calendar, Year, Month, Day) ->
    {Year, Month, Day}.
