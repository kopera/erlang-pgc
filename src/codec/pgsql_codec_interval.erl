-module(pgsql_codec_interval).
-behaviour(pgsql_codec).
-export([
    init/2,
    encodes/1,
    encode/4,
    decodes/1,
    decode/4
]).

-include("../../include/types.hrl").

init(_, Opts) ->
    maps:get(interval_format, Opts, record).

encodes(_Opts) ->
    [interval_send].

encode(_Type, Value, _Codec, Format) ->
    {Months, Days, MicroSecs} = input(Format, Value),
    <<MicroSecs:64/integer, Days:32/integer, Months:32/integer>>.

decodes(_Opts) ->
    [interval_recv].

decode(_Type, <<MicroSecs:64/integer, Days:32/integer, Months:32/integer>>, _Codec, Format) ->
    output(Format, Months, Days, MicroSecs).


% Internals

input(record, #pgsql_interval{months = Months, days = Days, micro_seconds = MicroSecs}) ->
    {Months, Days, MicroSecs};
input(map, #{months := Months, days := Days, micro_seconds := MicroSecs}) ->
    {Months, Days, MicroSecs};
input(_, Date) ->
    error(badarg, [Date]).

output(record, Months, Days, MicroSecs) ->
    #pgsql_interval{months = Months, days = Days, micro_seconds = MicroSecs};
output(map, Months, Days, MicroSecs) ->
    #{months => Months, days => Days, micro_seconds => MicroSecs}.
