-module(pgsql_codec_datetime).
-behaviour(pgsql_codec).
-export([
    encodes/1,
    encode/4,
    decodes/1,
    decode/4
]).

-include("../../include/types.hrl").
-define(epoch, 63113904000 * 1000000). % calendar:datetime_to_gregorian_seconds({{2000, 1, 1}, {0, 0, 0}}).

encodes(_Opts) ->
    [timestamp_send, timestamptz_send].

encode(_Type, #pgsql_datetime{year = Y, month = M, day = D, hours = Hr, minutes = Mn, seconds = Sc, micro_seconds = Ms}, _Codec, _Opts) ->
    Datetime = {{Y, M, D}, {Hr, Mn, Sc}},
    Timestamp = (calendar:datetime_to_gregorian_seconds(Datetime)) * 1000000 + Ms - ?epoch,
    <<Timestamp:64/signed-integer>>;
encode(_Type, Value, _Codec, _Opts) ->
    error(badarg, [Value]).

decodes(_Opts) ->
    [timestamp_recv, timestamptz_recv].

decode(_Type, <<Value:64/signed-integer>>, _Codec, _Opts) ->
    decode_datetime(Value + ?epoch).

%% Internals

decode_datetime(Value) ->
    Ms = Value rem 1000000,
    {{Y, M, D}, {Hr, Mn, Sc}} = calendar:gregorian_seconds_to_datetime((Value div 1000000)),
    #pgsql_datetime{year = Y, month = M, day = D, hours = Hr, minutes = Mn, seconds = Sc, micro_seconds = Ms}.
