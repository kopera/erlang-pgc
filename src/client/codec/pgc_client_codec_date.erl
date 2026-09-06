-module(pgc_client_codec_date).
-moduledoc false.

-behaviour(pgc_client_codec).
-export([
    names/0,
    encode/3,
    decode/3
]).

-define(epoch, 730485). % calendar:date_to_gregorian_days({2000, 1, 1})

names() ->
    [~"date_send", ~"date_recv"].

encode({_, _, _} = Date, _TypeDescriptor, _Codecs) ->
    case calendar:valid_date(Date) of
        true -> <<(calendar:date_to_gregorian_days(Date) - ?epoch):32/signed-integer>>;
        false -> erlang:error(badarg, [Date])
    end;
encode(Value, TypeDescriptor, Codecs) ->
    erlang:error(badarg, [Value, TypeDescriptor, Codecs]).

decode(<<Days:32/signed-integer>>, _TypeDescriptor, _Codecs) ->
    calendar:gregorian_days_to_date(Days + ?epoch).
