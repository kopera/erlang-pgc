-module(pgsql_codec_interval).
-behaviour(pgsql_codec).
-export([
    encodes/1,
    encode/4,
    decodes/1,
    decode/4
]).

-include("../../include/types.hrl").

encodes(_Opts) ->
    [<<"interval_send">>].

encode(_Type, #pgsql_interval{months = Months, days = Days, micro_seconds = MicroSecs}, _Codec, _Opts) ->
    <<MicroSecs:64/integer, Days:32/integer, Months:32/integer>>;
encode(_Type, Value, _Codec, _Opts) ->
    error(badarg, [Value]).

decodes(_Opts) ->
    [<<"interval_recv">>].

decode(_Type, <<MicroSecs:64/integer, Days:32/integer, Months:32/integer>>, _Codec, _Opts) ->
    #pgsql_interval{months = Months, days = Days, micro_seconds = MicroSecs}.

