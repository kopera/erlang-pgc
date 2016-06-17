-module(pgsql_codec_date).

-behaviour(pgsql_codec).
-export([
    encodes/1,
    encode/4,
    decodes/1,
    decode/4
]).

-include("../../include/types.hrl").
-define(epoch, 730485). % calendar:date_to_gregorian_days({2000, 1, 1})).

encodes(_Opts) ->
    [<<"date_send">>].

encode(_Type, #pgsql_date{year = Year, month = Month, day = Day} = D, _Codec, _Opts) ->
    Date = {Year, Month, Day},
    case calendar:valid_date(Date) of
        true -> <<(calendar:date_to_gregorian_days(Date) - ?epoch):32/signed-integer>>;
        false -> error(badarg, [D])
    end;
encode(_Type, Value, _Codec, _Opts) ->
    error(badarg, [Value]).

decodes(_Opts) ->
    [<<"date_recv">>].

decode(_Type, <<Days:32/signed-integer>>, _Codec, _Opts) ->
    {Year, Month, Day} = calendar:gregorian_days_to_date(Days + ?epoch),
    #pgsql_date{year = Year, month = Month, day = Day}.
