%% @private
-module(pgc_codec_interval).

-behaviour(pgc_codec).
-export([
    init/1,
    encode/2,
    decode/2
]).


init(_Options) ->
    Info = #{
        encodes => [interval_send],
        decodes => [interval_recv]
    },
    {Info, []}.


encode(Value, Options) when is_map(Value) ->
    {MicroSeconds, Days, Months} = from_term(Value, Options),
    <<MicroSeconds:64/signed-integer, Days:32/signed-integer, Months:32/signed-integer>>.


decode(<<MicroSeconds:64/signed-integer, Days:32/signed-integer, Months:32/signed-integer>>, Options) ->
    to_term(MicroSeconds, Days, Months, Options).


% ------------------------------------------------------------------------------
% Encoding
% ------------------------------------------------------------------------------

%% @private
from_term(Value, _Options) when is_map(Value) ->
    MicroSeconds = maps:get(microseconds, Value, 0),
    Days = maps:get(days, Value, 0),
    Months = maps:get(months, Value, 0),
    {MicroSeconds, Days, Months}.

% ------------------------------------------------------------------------------
% Decoding
% ------------------------------------------------------------------------------

%% @private
to_term(MicroSeconds, Days, Months, _Options) ->
    #{microseconds => MicroSeconds, days => Days, months => Months}.
