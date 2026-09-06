-module(pgc_client_codec_interval).
-moduledoc false.

-behaviour(pgc_client_codec).
-export([
    names/0,
    encode/3,
    decode/3
]).

names() ->
    [~"interval_send", ~"interval_recv"].

encode(Value, _TypeDescriptor, _Codecs) when is_map(Value) ->
    MicroSeconds = maps:get(microseconds, Value, 0),
    Days = maps:get(days, Value, 0),
    Months = maps:get(months, Value, 0),
    <<MicroSeconds:64/signed-integer, Days:32/signed-integer, Months:32/signed-integer>>;
encode(Value, TypeDescriptor, Codecs) ->
    erlang:error(badarg, [Value, TypeDescriptor, Codecs]).

decode(<<MicroSeconds:64/signed-integer, Days:32/signed-integer, Months:32/signed-integer>>, _TypeDescriptor, _Codecs) ->
    #{microseconds => MicroSeconds, days => Days, months => Months}.
