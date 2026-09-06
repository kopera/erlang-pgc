-module(pgc_client_codec_time).
-moduledoc false.

-behaviour(pgc_client_codec).
-export([
    names/0,
    encode/3,
    decode/3,
    from_term/2,
    to_term/2
]).

names() ->
    [~"time_send", ~"time_recv"].

-doc """
Representation comes from this call's `codecs => #{time => #{representation => Rep}}` option
(default `{system_time, native}`) -- `{calendar, time}` reads/writes a `calendar:time()` instead.
""".
encode(Term, _TypeDescriptor, Codecs) ->
    <<(from_term(Term, representation(Codecs))):64/signed-integer>>.

decode(<<MicroSeconds:64/signed-integer>>, _TypeDescriptor, Codecs) ->
    to_term(MicroSeconds, representation(Codecs)).

representation(Codecs) ->
    maps:get(representation, pgc_client_codecs:options(time, Codecs), {system_time, native}).

from_term(Term, {calendar, time}) ->
    erlang:convert_time_unit(calendar:time_to_seconds(Term), second, microsecond);
from_term(Term, {system_time, Unit}) ->
    erlang:convert_time_unit(Term, Unit, microsecond).

to_term(MicroSeconds, {calendar, time}) ->
    calendar:seconds_to_time(erlang:convert_time_unit(MicroSeconds, microsecond, second));
to_term(MicroSeconds, {system_time, Unit}) ->
    erlang:convert_time_unit(MicroSeconds, microsecond, Unit).
