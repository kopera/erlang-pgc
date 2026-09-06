-module(pgc_client_codec_builtin).
-moduledoc false.

-export([
    boolsend/3, boolrecv/3,
    int2send/3, int2recv/3,
    int4send/3, int4recv/3,
    int8send/3, int8recv/3,
    float4send/3, float4recv/3,
    float8send/3, float8recv/3,
    byteasend/3, bytearecv/3, unknownsend/3, unknownrecv/3,
    uuid_send/3, uuid_recv/3,
    textsend/3, textrecv/3, varcharsend/3, varcharrecv/3, bpcharsend/3, bpcharrecv/3, citextsend/3, citextrecv/3,
    charsend/3, charrecv/3,
    date_send/3, date_recv/3,
    namesend/3, namerecv/3,
    oidsend/3, oidrecv/3,
    xidsend/3, xidrecv/3,
    cidsend/3, cidrecv/3,
    regprocsend/3, regprocrecv/3,
    regproceduresend/3, regprocedurerecv/3,
    regopersend/3, regoperrecv/3,
    regoperatorsend/3, regoperatorrecv/3,
    regclasssend/3, regclassrecv/3,
    regtypesend/3, regtyperecv/3,
    tidsend/3, tidrecv/3,
    void_send/3, void_recv/3,
    xid8send/3, xid8recv/3,
    bit_send/3, bit_recv/3, varbit_send/3, varbit_recv/3,
    ltree_send/3, ltreesend/3, ltree_recv/3, ltreerecv/3, lquery_send/3, lquery_recv/3,
    hstore_send/3, hstore_recv/3,
    interval_send/3, interval_recv/3,
    enum_send/3, enum_recv/3,
    time_send/3, time_recv/3,
    timetz_send/3, timetz_recv/3,
    timestamp_send/3, timestamp_recv/3, timestamptz_send/3, timestamptz_recv/3,
    json_send/3, json_recv/3,
    jsonb_send/3, jsonb_recv/3,
    array_send/3, array_recv/3, int2vectorsend/3, int2vectorrecv/3, oidvectorsend/3, oidvectorrecv/3,
    record_send/3, record_recv/3,
    range_send/3, range_recv/3,
    multirange_send/3, multirange_recv/3
]).

% ------------------------------------------------------------------------------
% bool
% ------------------------------------------------------------------------------

boolsend(true, _TypeDescriptor, _Codecs) -> <<1>>;
boolsend(false, _TypeDescriptor, _Codecs) -> <<0>>;
boolsend(Value, TypeDescriptor, Codecs) -> erlang:error(badarg, [Value, TypeDescriptor, Codecs]).

boolrecv(<<1>>, _TypeDescriptor, _Codecs) -> true;
boolrecv(<<0>>, _TypeDescriptor, _Codecs) -> false.

% ------------------------------------------------------------------------------
% int2 / int4 / int8
% ------------------------------------------------------------------------------

int2send(Value, _TypeDescriptor, _Codecs) when is_integer(Value), Value >= -32768, Value =< 32767 ->
    <<Value:16/signed-integer>>;
int2send(Value, TypeDescriptor, Codecs) ->
    erlang:error(badarg, [Value, TypeDescriptor, Codecs]).

int2recv(<<Value:16/signed-integer>>, _TypeDescriptor, _Codecs) -> Value.

int4send(Value, _TypeDescriptor, _Codecs) when is_integer(Value), Value >= -2147483648, Value =< 2147483647 ->
    <<Value:32/signed-integer>>;
int4send(Value, TypeDescriptor, Codecs) ->
    erlang:error(badarg, [Value, TypeDescriptor, Codecs]).

int4recv(<<Value:32/signed-integer>>, _TypeDescriptor, _Codecs) -> Value.

int8send(Value, _TypeDescriptor, _Codecs) when is_integer(Value), Value >= -9223372036854775808, Value =< 9223372036854775807 ->
    <<Value:64/signed-integer>>;
int8send(Value, TypeDescriptor, Codecs) ->
    erlang:error(badarg, [Value, TypeDescriptor, Codecs]).

int8recv(<<Value:64/signed-integer>>, _TypeDescriptor, _Codecs) -> Value.

% ------------------------------------------------------------------------------
% float4 / float8
% ------------------------------------------------------------------------------

float4send('NaN', _TypeDescriptor, _Codecs) -> <<127, 192, 0, 0>>;
float4send(infinity, _TypeDescriptor, _Codecs) -> <<127, 128, 0, 0>>;
float4send('-infinity', _TypeDescriptor, _Codecs) -> <<255, 128, 0, 0>>;
float4send(Value, _TypeDescriptor, _Codecs) when is_number(Value) -> <<Value:32/signed-float>>;
float4send(Value, TypeDescriptor, Codecs) -> erlang:error(badarg, [Value, TypeDescriptor, Codecs]).

float4recv(<<127, 192, 0, 0>>, _TypeDescriptor, _Codecs) -> 'NaN';
float4recv(<<127, 128, 0, 0>>, _TypeDescriptor, _Codecs) -> infinity;
float4recv(<<255, 128, 0, 0>>, _TypeDescriptor, _Codecs) -> '-infinity';
float4recv(<<Value:32/signed-float>>, _TypeDescriptor, _Codecs) -> Value.

float8send('NaN', _TypeDescriptor, _Codecs) -> <<127, 248, 0, 0, 0, 0, 0, 0>>;
float8send(infinity, _TypeDescriptor, _Codecs) -> <<127, 240, 0, 0, 0, 0, 0, 0>>;
float8send('-infinity', _TypeDescriptor, _Codecs) -> <<255, 240, 0, 0, 0, 0, 0, 0>>;
float8send(Value, _TypeDescriptor, _Codecs) when is_number(Value) -> <<Value:64/signed-float>>;
float8send(Value, TypeDescriptor, Codecs) -> erlang:error(badarg, [Value, TypeDescriptor, Codecs]).

float8recv(<<127, 248, 0, 0, 0, 0, 0, 0>>, _TypeDescriptor, _Codecs) -> 'NaN';
float8recv(<<127, 240, 0, 0, 0, 0, 0, 0>>, _TypeDescriptor, _Codecs) -> infinity;
float8recv(<<255, 240, 0, 0, 0, 0, 0, 0>>, _TypeDescriptor, _Codecs) -> '-infinity';
float8recv(<<Value:64/signed-float>>, _TypeDescriptor, _Codecs) -> Value.

% ------------------------------------------------------------------------------
% bytea (+ unknown, which is wire-identical)
% ------------------------------------------------------------------------------

byteasend(Value, _TypeDescriptor, _Codecs) -> _ = iolist_size(Value), Value.
bytearecv(Data, _TypeDescriptor, _Codecs) when is_binary(Data) -> Data.
unknownsend(Value, TypeDescriptor, Codecs) -> byteasend(Value, TypeDescriptor, Codecs).
unknownrecv(Data, TypeDescriptor, Codecs) -> bytearecv(Data, TypeDescriptor, Codecs).

% ------------------------------------------------------------------------------
% uuid
% ------------------------------------------------------------------------------

uuid_send(<<_:128>> = Uuid, _TypeDescriptor, _Codecs) -> Uuid;
uuid_send(Value, TypeDescriptor, Codecs) -> erlang:error(badarg, [Value, TypeDescriptor, Codecs]).

uuid_recv(<<_:128>> = Uuid, _TypeDescriptor, _Codecs) -> Uuid.

% ------------------------------------------------------------------------------
% text (+ varchar/bpchar/citext, all wire-identical pass-through)
% ------------------------------------------------------------------------------

textsend(Value, _TypeDescriptor, _Codecs) -> _ = iolist_size(Value), Value.
textrecv(Data, _TypeDescriptor, _Codecs) -> Data.
varcharsend(Value, TypeDescriptor, Codecs) -> textsend(Value, TypeDescriptor, Codecs).
varcharrecv(Data, TypeDescriptor, Codecs) -> textrecv(Data, TypeDescriptor, Codecs).
bpcharsend(Value, TypeDescriptor, Codecs) -> textsend(Value, TypeDescriptor, Codecs).
bpcharrecv(Data, TypeDescriptor, Codecs) -> textrecv(Data, TypeDescriptor, Codecs).
citextsend(Value, TypeDescriptor, Codecs) -> textsend(Value, TypeDescriptor, Codecs).
citextrecv(Data, TypeDescriptor, Codecs) -> textrecv(Data, TypeDescriptor, Codecs).

% ------------------------------------------------------------------------------
% char
% ------------------------------------------------------------------------------

charsend(Char, _TypeDescriptor, _Codecs) when is_integer(Char), Char >= 0, Char < 256 -> <<Char>>;
charsend(Value, TypeDescriptor, Codecs) -> erlang:error(badarg, [Value, TypeDescriptor, Codecs]).

charrecv(<<Char>>, _TypeDescriptor, _Codecs) -> Char.

% ------------------------------------------------------------------------------
% date
% ------------------------------------------------------------------------------

-define(date_epoch, 730485). % calendar:date_to_gregorian_days({2000, 1, 1})

date_send({_, _, _} = Date, _TypeDescriptor, _Codecs) ->
    case calendar:valid_date(Date) of
        true -> <<(calendar:date_to_gregorian_days(Date) - ?date_epoch):32/signed-integer>>;
        false -> erlang:error(badarg, [Date])
    end;
date_send(Value, TypeDescriptor, Codecs) ->
    erlang:error(badarg, [Value, TypeDescriptor, Codecs]).

date_recv(<<Days:32/signed-integer>>, _TypeDescriptor, _Codecs) ->
    calendar:gregorian_days_to_date(Days + ?date_epoch).

% ------------------------------------------------------------------------------
% name
% ------------------------------------------------------------------------------

namesend(Value, _TypeDescriptor, _Codecs) when is_binary(Value), byte_size(Value) < 64 -> Value;
namesend(Value, TypeDescriptor, Codecs) -> erlang:error(badarg, [Value, TypeDescriptor, Codecs]).

namerecv(Data, _TypeDescriptor, _Codecs) -> Data.

% ------------------------------------------------------------------------------
% oid family -- oid/xid/cid/regproc/regprocedure/regoper/regoperator/regclass/regtype all
% share the plain uint32 wire representation.
% ------------------------------------------------------------------------------

oid_send(Value, _TypeDescriptor, _Codecs) when is_integer(Value), Value >= 0, Value =< 4294967295 ->
    <<Value:32/integer>>;
oid_send(Value, TypeDescriptor, Codecs) ->
    erlang:error(badarg, [Value, TypeDescriptor, Codecs]).

oid_recv(<<Value:32/integer>>, _TypeDescriptor, _Codecs) -> Value.

oidsend(V, T, C) -> oid_send(V, T, C).
oidrecv(V, T, C) -> oid_recv(V, T, C).
xidsend(V, T, C) -> oid_send(V, T, C).
xidrecv(V, T, C) -> oid_recv(V, T, C).
cidsend(V, T, C) -> oid_send(V, T, C).
cidrecv(V, T, C) -> oid_recv(V, T, C).
regprocsend(V, T, C) -> oid_send(V, T, C).
regprocrecv(V, T, C) -> oid_recv(V, T, C).
regproceduresend(V, T, C) -> oid_send(V, T, C).
regprocedurerecv(V, T, C) -> oid_recv(V, T, C).
regopersend(V, T, C) -> oid_send(V, T, C).
regoperrecv(V, T, C) -> oid_recv(V, T, C).
regoperatorsend(V, T, C) -> oid_send(V, T, C).
regoperatorrecv(V, T, C) -> oid_recv(V, T, C).
regclasssend(V, T, C) -> oid_send(V, T, C).
regclassrecv(V, T, C) -> oid_recv(V, T, C).
regtypesend(V, T, C) -> oid_send(V, T, C).
regtyperecv(V, T, C) -> oid_recv(V, T, C).

% ------------------------------------------------------------------------------
% tid
% ------------------------------------------------------------------------------

tidsend({Block, Tuple}, _TypeDescriptor, _Codecs)
        when is_integer(Block), Block >= 0, Block =< 4294967295, is_integer(Tuple), Tuple >= 0, Tuple =< 65535 ->
    <<Block:32/integer, Tuple:16/integer>>;
tidsend(Value, TypeDescriptor, Codecs) ->
    erlang:error(badarg, [Value, TypeDescriptor, Codecs]).

tidrecv(<<Block:32/integer, Tuple:16/integer>>, _TypeDescriptor, _Codecs) -> {Block, Tuple}.

% ------------------------------------------------------------------------------
% void
% ------------------------------------------------------------------------------

void_send(undefined, _TypeDescriptor, _Codecs) -> <<>>;
void_send(Value, TypeDescriptor, Codecs) -> erlang:error(badarg, [Value, TypeDescriptor, Codecs]).

void_recv(<<>>, _TypeDescriptor, _Codecs) -> undefined.

% ------------------------------------------------------------------------------
% xid8
% ------------------------------------------------------------------------------

xid8send(Value, _TypeDescriptor, _Codecs) when is_integer(Value), Value >= 0, Value =< 18_446_744_073_709_551_615 ->
    <<Value:64/integer>>;
xid8send(Value, TypeDescriptor, Codecs) ->
    erlang:error(badarg, [Value, TypeDescriptor, Codecs]).

xid8recv(<<Value:64/integer>>, _TypeDescriptor, _Codecs) -> Value.

% ------------------------------------------------------------------------------
% bitstring (bit/varbit)
% ------------------------------------------------------------------------------

bitstring_send(Value, _TypeDescriptor, _Codecs) when is_bitstring(Value) ->
    Size = bit_size(Value),
    Padding = (8 - (Size rem 8)) rem 8,
    <<Size:32/integer, Value/bitstring, 0:Padding>>;
bitstring_send(Value, TypeDescriptor, Codecs) ->
    erlang:error(badarg, [Value, TypeDescriptor, Codecs]).

bitstring_recv(<<Size:32/integer, Bits:Size/bits, _/bits>>, _TypeDescriptor, _Codecs) -> Bits.

bit_send(V, T, C) -> bitstring_send(V, T, C).
bit_recv(V, T, C) -> bitstring_recv(V, T, C).
varbit_send(V, T, C) -> bitstring_send(V, T, C).
varbit_recv(V, T, C) -> bitstring_recv(V, T, C).

% ------------------------------------------------------------------------------
% ltree (+ lquery, wire-identical)
% ------------------------------------------------------------------------------

ltree_send(Value, _TypeDescriptor, _Codecs) when is_binary(Value) -> <<1:8, Value/binary>>;
ltree_send(Value, TypeDescriptor, Codecs) -> erlang:error(badarg, [Value, TypeDescriptor, Codecs]).

ltreesend(Value, TypeDescriptor, Codecs) -> ltree_send(Value, TypeDescriptor, Codecs).

ltree_recv(<<1:8, Value/binary>>, _TypeDescriptor, _Codecs) -> Value.

ltreerecv(Value, TypeDescriptor, Codecs) -> ltree_recv(Value, TypeDescriptor, Codecs).

lquery_send(V, T, C) -> ltree_send(V, T, C).
lquery_recv(V, T, C) -> ltree_recv(V, T, C).

% ------------------------------------------------------------------------------
% hstore
% ------------------------------------------------------------------------------

hstore_send(Value, _TypeDescriptor, _Codecs) when is_map(Value) ->
    [<<(map_size(Value)):32/integer>>, maps:fold(fun (Key, Val, Acc) ->
        [hstore_encode_key(Key), hstore_encode_value(Val) | Acc]
    end, [], Value)];
hstore_send(Value, TypeDescriptor, Codecs) ->
    erlang:error(badarg, [Value, TypeDescriptor, Codecs]).

hstore_encode_key(Key) when is_binary(Key) ->
    <<(byte_size(Key)):32/integer, Key/binary>>;
hstore_encode_key(Key) ->
    erlang:error(badarg, [Key]).

hstore_encode_value(null) ->
    <<-1:32/signed-integer>>;
hstore_encode_value(Value) when is_binary(Value) ->
    <<(byte_size(Value)):32/signed-integer, Value/binary>>;
hstore_encode_value(Value) ->
    erlang:error(badarg, [Value]).

hstore_recv(<<_Size:32/integer, Payload/binary>>, _TypeDescriptor, _Codecs) ->
    hstore_decode_pairs(Payload, #{}).

hstore_decode_pairs(<<KeyLength:32/integer, Key:KeyLength/binary, -1:32/signed-integer, Rest/binary>>, Acc) ->
    hstore_decode_pairs(Rest, Acc#{Key => null});
hstore_decode_pairs(<<KeyLength:32/integer, Key:KeyLength/binary, ValueLength:32/signed-integer, Value:ValueLength/binary, Rest/binary>>, Acc) ->
    hstore_decode_pairs(Rest, Acc#{Key => Value});
hstore_decode_pairs(<<>>, Acc) ->
    Acc.

% ------------------------------------------------------------------------------
% interval
% ------------------------------------------------------------------------------

interval_send(Value, _TypeDescriptor, _Codecs) when is_map(Value) ->
    MicroSeconds = maps:get(microseconds, Value, 0),
    Days = maps:get(days, Value, 0),
    Months = maps:get(months, Value, 0),
    <<MicroSeconds:64/signed-integer, Days:32/signed-integer, Months:32/signed-integer>>;
interval_send(Value, TypeDescriptor, Codecs) ->
    erlang:error(badarg, [Value, TypeDescriptor, Codecs]).

interval_recv(<<MicroSeconds:64/signed-integer, Days:32/signed-integer, Months:32/signed-integer>>, _TypeDescriptor, _Codecs) ->
    #{microseconds => MicroSeconds, days => Days, months => Months}.

% ------------------------------------------------------------------------------
% enum
% ------------------------------------------------------------------------------

enum_send(Term, _TypeDescriptor, _Codecs) when is_atom(Term) ->
    atom_to_binary(Term);
enum_send(Term, TypeDescriptor, Codecs) ->
    case unicode:characters_to_binary(Term) of
        Value when is_binary(Value) -> Value;
        _ -> erlang:error(badarg, [Term, TypeDescriptor, Codecs])
    end.

-doc """
Decode mode comes from this call's `codecs => #{enum => #{decode => Mode}}` option (default
`binary`) -- `atom` and `existing_atom` are for callers who know the enum's full value set ahead
of time and want it back as an atom rather than doing that conversion themselves on every row.
""".
enum_recv(Data, _TypeDescriptor, Codecs) ->
    case maps:get(decode, pgc_client_codec:options(enum, Codecs), binary) of
        binary -> Data;
        % elp:ignore W0023 -- opt-in, caller-documented tradeoff (see -doc above)
        atom -> binary_to_atom(Data);
        existing_atom -> binary_to_existing_atom(Data)
    end.

% ------------------------------------------------------------------------------
% time / timetz
% ------------------------------------------------------------------------------

-doc """
Representation comes from this call's `codecs => #{time => #{representation => Rep}}` option
(default `{system_time, native}`) -- `{calendar, time}` reads/writes a `calendar:time()` instead.
Shared by `time` and `timetz` (whose wire time zone offset isn't represented in either term
shape, and is always encoded as `0`).
""".
time_send(Term, _TypeDescriptor, Codecs) ->
    <<(time_from_term(Term, time_representation(Codecs))):64/signed-integer>>.

time_recv(<<MicroSeconds:64/signed-integer>>, _TypeDescriptor, Codecs) ->
    time_to_term(MicroSeconds, time_representation(Codecs)).

timetz_send(Term, _TypeDescriptor, Codecs) ->
    MicroSeconds = time_from_term(Term, time_representation(Codecs)),
    <<MicroSeconds:64/signed-integer, 0:32/signed-integer>>.

timetz_recv(<<MicroSeconds:64/signed-integer, Offset:32/signed-integer>>, _TypeDescriptor, Codecs) ->
    time_to_term(MicroSeconds + Offset * 1_000_000, time_representation(Codecs)).

time_representation(Codecs) ->
    maps:get(representation, pgc_client_codec:options(time, Codecs), {system_time, native}).

time_from_term(Term, {calendar, time}) ->
    erlang:convert_time_unit(calendar:time_to_seconds(Term), second, microsecond);
time_from_term(Term, {system_time, Unit}) ->
    erlang:convert_time_unit(Term, Unit, microsecond).

time_to_term(MicroSeconds, {calendar, time}) ->
    calendar:seconds_to_time(erlang:convert_time_unit(MicroSeconds, microsecond, second));
time_to_term(MicroSeconds, {system_time, Unit}) ->
    erlang:convert_time_unit(MicroSeconds, microsecond, Unit).

% ------------------------------------------------------------------------------
% timestamp (+ timestamptz, wire-identical)
% ------------------------------------------------------------------------------

-define(posix_epoch, 62167219200). % calendar:datetime_to_gregorian_seconds({{1970, 1, 1}, {0, 0, 0}})
-define(pg_epoch,    63113904000). % calendar:datetime_to_gregorian_seconds({{2000, 1, 1}, {0, 0, 0}})

-doc """
Representation comes from this call's `codecs => #{timestamp => #{representation => Rep}}`
option (default `{system_time, native}`) -- `{calendar, datetime}` reads/writes a
`calendar:datetime()` instead. `infinity`/`'-infinity'` round-trip as themselves either way.
""".
timestamp_send(infinity, _TypeDescriptor, _Codecs) ->
    <<16#7FFFFFFFFFFFFFFF:64/signed-integer>>;
timestamp_send('-infinity', _TypeDescriptor, _Codecs) ->
    <<-16#8000000000000000:64/signed-integer>>;
timestamp_send(Term, _TypeDescriptor, Codecs) ->
    <<(timestamp_from_term(Term, timestamp_representation(Codecs))):64/signed-integer>>.

timestamp_recv(<<16#7FFFFFFFFFFFFFFF:64/signed-integer>>, _TypeDescriptor, _Codecs) ->
    infinity;
timestamp_recv(<<-16#8000000000000000:64/signed-integer>>, _TypeDescriptor, _Codecs) ->
    '-infinity';
timestamp_recv(<<PGMicroSeconds:64/signed-integer>>, _TypeDescriptor, Codecs) ->
    timestamp_to_term(PGMicroSeconds, timestamp_representation(Codecs)).

timestamptz_send(V, T, C) -> timestamp_send(V, T, C).
timestamptz_recv(V, T, C) -> timestamp_recv(V, T, C).

timestamp_representation(Codecs) ->
    maps:get(representation, pgc_client_codec:options(timestamp, Codecs), {system_time, native}).

timestamp_from_term(Term, {calendar, datetime}) ->
    GregorianSeconds = calendar:datetime_to_gregorian_seconds(Term),
    erlang:convert_time_unit(GregorianSeconds - ?pg_epoch, second, microsecond);
timestamp_from_term(Term, {system_time, Unit}) when is_integer(Term) ->
    GregorianNativeTime = erlang:convert_time_unit(Term, Unit, native) + erlang:convert_time_unit(?posix_epoch, second, native),
    PGNativeTime = GregorianNativeTime - erlang:convert_time_unit(?pg_epoch, second, native),
    erlang:convert_time_unit(PGNativeTime, native, microsecond).

timestamp_to_term(PGMicroSeconds, {calendar, datetime}) ->
    GregorianSeconds = erlang:convert_time_unit(PGMicroSeconds, microsecond, second) + ?pg_epoch,
    calendar:gregorian_seconds_to_datetime(GregorianSeconds);
timestamp_to_term(PGMicroSeconds, {system_time, Unit}) ->
    GregorianMicroSeconds = PGMicroSeconds + erlang:convert_time_unit(?pg_epoch, second, microsecond),
    PosixMicroSeconds = GregorianMicroSeconds - erlang:convert_time_unit(?posix_epoch, second, microsecond),
    erlang:convert_time_unit(PosixMicroSeconds, microsecond, Unit).

% ------------------------------------------------------------------------------
% json / jsonb
% ------------------------------------------------------------------------------

-doc """
Encodes/decodes through OTP's own `json` module by default, pass a
`codecs => #{json => #{codec => Module}}` option to plug in something else
instead (`Module:encode/1`/`Module:decode/1`, same contract `json` itself meets).
""".
json_send(Term, _TypeDescriptor, Codecs) ->
    Module = maps:get(codec, pgc_client_codec:options(json, Codecs), json),
    Module:encode(Term).

json_recv(Data, _TypeDescriptor, Codecs) ->
    Module = maps:get(codec, pgc_client_codec:options(json, Codecs), json),
    Module:decode(Data).

jsonb_send(Term, TypeDescriptor, Codecs) ->
    [1 | json_send(Term, TypeDescriptor, Codecs)].

jsonb_recv(<<1, Data/binary>>, TypeDescriptor, Codecs) ->
    json_recv(Data, TypeDescriptor, Codecs).

% ------------------------------------------------------------------------------
% array / record / range / multirange -- complex binary layouts, kept in their own modules
% ------------------------------------------------------------------------------

array_send(V, {_Oid, _Name, _Kind, _Recv, _Send, ElementOid, _Parent, _Fields}, C) ->
    {ok, ElementDescriptor} = pgc_client_codec:lookup(ElementOid, C),
    pgc_client_codec_array:encode(V, ElementOid, fun(Value) -> pgc_client_codec:encode(Value, ElementDescriptor, C) end).
array_recv(V, {_Oid, _Name, _Kind, _Recv, _Send, ElementOid, _Parent, _Fields}, C) ->
    {ok, ElementDescriptor} = pgc_client_codec:lookup(ElementOid, C),
    pgc_client_codec_array:decode(V, fun(Data) -> pgc_client_codec:decode(Data, ElementDescriptor, C) end).
int2vectorsend(V, T, C) -> array_send(V, T, C).
int2vectorrecv(V, T, C) -> array_recv(V, T, C).
oidvectorsend(V, T, C) -> array_send(V, T, C).
oidvectorrecv(V, T, C) -> array_recv(V, T, C).

record_send(V, {_Oid, _Name, _Kind, _Recv, _Send, _Element, _Parent, FieldsDescription}, C) ->
    pgc_client_codec_record:encode(V, FieldsDescription, fun(Oid, Value) ->
        {ok, Descriptor} = pgc_client_codec:lookup(Oid, C),
        pgc_client_codec:encode(Value, Descriptor, C)
    end).
record_recv(V, {_Oid, _Name, _Kind, _Recv, _Send, _Element, _Parent, FieldsDescription}, C) ->
    map = maps:get(decode, pgc_client_codec:options(record, C), map),
    pgc_client_codec_record:decode(V, FieldsDescription, fun(Oid, Data) ->
        {ok, Descriptor} = pgc_client_codec:lookup(Oid, C),
        pgc_client_codec:decode(Data, Descriptor, C)
    end).

range_send(V, {_Oid, _Name, _Kind, _Recv, _Send, _Element, Parent, _Fields}, C) ->
    {ok, ElementDescriptor} = pgc_client_codec:lookup(Parent, C),
    pgc_client_codec_range:encode(V, fun(Value) -> pgc_client_codec:encode(Value, ElementDescriptor, C) end).
range_recv(V, {_Oid, _Name, _Kind, _Recv, _Send, _Element, Parent, _Fields}, C) ->
    {ok, ElementDescriptor} = pgc_client_codec:lookup(Parent, C),
    {Range, <<>>} = pgc_client_codec_range:decode(V, fun(Data) -> pgc_client_codec:decode(Data, ElementDescriptor, C) end),
    Range.

multirange_send(V, {_Oid, _Name, _Kind, _Recv, _Send, _Element, Parent, _Fields}, C) ->
    {ok, ElementDescriptor} = pgc_client_codec:lookup(Parent, C),
    pgc_client_codec_multirange:encode(V, fun(Value) -> pgc_client_codec:encode(Value, ElementDescriptor, C) end).
multirange_recv(V, {_Oid, _Name, _Kind, _Recv, _Send, _Element, Parent, _Fields}, C) ->
    {ok, ElementDescriptor} = pgc_client_codec:lookup(Parent, C),
    pgc_client_codec_multirange:decode(V, fun(Data) -> pgc_client_codec:decode(Data, ElementDescriptor, C) end).
