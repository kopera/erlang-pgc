-module(pgc_codec_timestamp).

-behaviour(pgc_codec).
-export([
    init/1,
    encode/2,
    decode/2
]).

-define(posix_epoch, 62167219200). % calendar:datetime_to_gregorian_seconds({{1970, 1, 1}, {0, 0, 0}}).
-define(pg_epoch,    63113904000). % calendar:datetime_to_gregorian_seconds({{2000, 1, 1}, {0, 0, 0}}).


init(Options) ->
    Codec = case Options of
        #{timestamp := {calendar, datetime}} -> {calendar, datetime};
        #{} -> {system_time, native}
    end,
    Info = #{
        encodes => [timestamp_send, timestamptz_send],
        decodes => [timestamp_recv, timestamptz_recv]
    },
    {Info, Codec}.


encode(infinity, _Codec) ->
    <<16#7FFFFFFFFFFFFFFF:64/signed-integer>>;
encode('-infinity', _Codec) ->
    <<-16#8000000000000000:64/signed-integer>>;
encode(Term, Codec) ->
    PGTime = from_term(Term, Codec),
    <<PGTime:64/signed-integer>>.


decode(<<16#7FFFFFFFFFFFFFFF:64/signed-integer>>, _Codec) ->
    infinity;
decode(<<-16#8000000000000000:64/signed-integer>>, _Codec) ->
    '-infinity';
decode(<<PGMicroSeconds:64/signed-integer>>, Codec) ->
    to_term(PGMicroSeconds, Codec).


% ------------------------------------------------------------------------------
% Encoding
% ------------------------------------------------------------------------------

%% @private
from_term(Term, {calendar, datetime}) ->
    GregorianSeconds = calendar:datetime_to_gregorian_seconds(Term),
    % PosixSeconds = Seconds - ?posix_epoch,
    PGSeconds = GregorianSeconds - ?pg_epoch,
    erlang:convert_time_unit(PGSeconds, second, microsecond);
from_term(Term, {system_time, TimeUnit}) when is_integer(Term) ->
    GregorianNativeTime = erlang:convert_time_unit(Term, TimeUnit, native) + erlang:convert_time_unit(?posix_epoch, second, native),
    PGNativeTime = GregorianNativeTime - erlang:convert_time_unit(?pg_epoch, second, native),
    erlang:convert_time_unit(PGNativeTime, native, microsecond);
from_term(Term, Codec) ->
    error(badarg, [Term, Codec]).


% ------------------------------------------------------------------------------
% Decoding
% ------------------------------------------------------------------------------

%% @private
to_term(PGMicroSeconds, {calendar, datetime}) ->
    GregorianSeconds = erlang:convert_time_unit(PGMicroSeconds, microsecond, second) + ?pg_epoch,
    calendar:gregorian_seconds_to_datetime(GregorianSeconds);
to_term(PGMicroSeconds, {system_time, TimeUnit}) ->
    GregorianMicroSeconds = PGMicroSeconds + erlang:convert_time_unit(?pg_epoch, second, microsecond),
    PosixMicroSeconds = GregorianMicroSeconds - erlang:convert_time_unit(?posix_epoch, second, microsecond),
    erlang:convert_time_unit(PosixMicroSeconds, microsecond, TimeUnit).