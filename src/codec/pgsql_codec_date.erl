-module(pgsql_codec_date).

-behaviour(pgsql_codec).
-export([
    encodes/1,
    encode/4,
    decodes/1,
    decode/4
]).

-define(year_max, 5874897).
-define(epoch, 730485). % calendar:date_to_gregorian_days({2000, 1, 1})).

encodes(_Opts) ->
    [<<"date_send">>].

encode(_Type, {Year, _Month, _Day} = Date, _Codec, _Opts) when is_integer(Year), Year =< ?year_max ->
    case calendar:valid_date(Date) of
        true -> <<(calendar:date_to_gregorian_days(Date) - ?epoch):32/signed-integer>>;
        false -> error(badarg, [Date])
    end;
encode(_Type, Value, _Codec, _Opts) ->
    error(badarg, [Value]).

decodes(_Opts) ->
    [<<"date_recv">>].

decode(_Type, <<Days:32/signed-integer>>, _Codec, _Opts) ->
    calendar:gregorian_days_to_date(Days + ?epoch).
