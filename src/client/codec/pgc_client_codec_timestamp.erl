-module(pgc_client_codec_timestamp).
-moduledoc false.

-behaviour(pgc_client_codec).
-export([
    names/0,
    encode/3,
    decode/3
]).

-define(posix_epoch, 62167219200). % calendar:datetime_to_gregorian_seconds({{1970, 1, 1}, {0, 0, 0}})
-define(pg_epoch,    63113904000). % calendar:datetime_to_gregorian_seconds({{2000, 1, 1}, {0, 0, 0}})

names() ->
    [~"timestamp_send", ~"timestamp_recv", ~"timestamptz_send", ~"timestamptz_recv"].

-doc """
Representation comes from this call's `codecs => #{timestamp => #{representation => Rep}}`
option (default `{system_time, native}`) -- `{calendar, datetime}` reads/writes a
`calendar:datetime()` instead. `infinity`/`'-infinity'` round-trip as themselves either way.
""".
encode(infinity, _TypeDescriptor, _Codecs) ->
    <<16#7FFFFFFFFFFFFFFF:64/signed-integer>>;
encode('-infinity', _TypeDescriptor, _Codecs) ->
    <<-16#8000000000000000:64/signed-integer>>;
encode(Term, _TypeDescriptor, Codecs) ->
    <<(from_term(Term, representation(Codecs))):64/signed-integer>>.

decode(<<16#7FFFFFFFFFFFFFFF:64/signed-integer>>, _TypeDescriptor, _Codecs) ->
    infinity;
decode(<<-16#8000000000000000:64/signed-integer>>, _TypeDescriptor, _Codecs) ->
    '-infinity';
decode(<<PGMicroSeconds:64/signed-integer>>, _TypeDescriptor, Codecs) ->
    to_term(PGMicroSeconds, representation(Codecs)).

representation(Codecs) ->
    maps:get(representation, pgc_client_codecs:options(timestamp, Codecs), {system_time, native}).

from_term(Term, {calendar, datetime}) ->
    GregorianSeconds = calendar:datetime_to_gregorian_seconds(Term),
    erlang:convert_time_unit(GregorianSeconds - ?pg_epoch, second, microsecond);
from_term(Term, {system_time, Unit}) when is_integer(Term) ->
    GregorianNativeTime = erlang:convert_time_unit(Term, Unit, native) + erlang:convert_time_unit(?posix_epoch, second, native),
    PGNativeTime = GregorianNativeTime - erlang:convert_time_unit(?pg_epoch, second, native),
    erlang:convert_time_unit(PGNativeTime, native, microsecond).

to_term(PGMicroSeconds, {calendar, datetime}) ->
    GregorianSeconds = erlang:convert_time_unit(PGMicroSeconds, microsecond, second) + ?pg_epoch,
    calendar:gregorian_seconds_to_datetime(GregorianSeconds);
to_term(PGMicroSeconds, {system_time, Unit}) ->
    GregorianMicroSeconds = PGMicroSeconds + erlang:convert_time_unit(?pg_epoch, second, microsecond),
    PosixMicroSeconds = GregorianMicroSeconds - erlang:convert_time_unit(?posix_epoch, second, microsecond),
    erlang:convert_time_unit(PosixMicroSeconds, microsecond, Unit).
