-module(pgc_client_codec_builtin).
-moduledoc false.

-export([
    boolsend/1, boolrecv/1,
    int2send/1, int2recv/1,
    int4send/1, int4recv/1,
    int8send/1, int8recv/1,
    float4send/1, float4recv/1,
    float8send/1, float8recv/1,
    byteasend/1, bytearecv/1, unknownsend/1, unknownrecv/1,
    uuid_send/1, uuid_recv/1,
    textsend/1, textrecv/1, varcharsend/1, varcharrecv/1, bpcharsend/1, bpcharrecv/1, citextsend/1, citextrecv/1,
    charsend/1, charrecv/1,
    date_send/1, date_recv/1,
    namesend/1, namerecv/1,
    oidsend/1, oidrecv/1,
    xidsend/1, xidrecv/1,
    cidsend/1, cidrecv/1,
    regprocsend/1, regprocrecv/1,
    regproceduresend/1, regprocedurerecv/1,
    regopersend/1, regoperrecv/1,
    regoperatorsend/1, regoperatorrecv/1,
    regclasssend/1, regclassrecv/1,
    regtypesend/1, regtyperecv/1,
    tidsend/1, tidrecv/1,
    void_send/1, void_recv/1,
    xid8send/1, xid8recv/1,
    bit_send/1, bit_recv/1, varbit_send/1, varbit_recv/1,
    ltree_send/1, ltreesend/1, ltree_recv/1, ltreerecv/1, lquery_send/1, lquery_recv/1,
    hstore_send/1, hstore_recv/1,
    interval_send/1, interval_recv/1,
    enum_send/1, enum_recv/3,
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

-import_record(pgc_client_codec, [codec]).
-import_record(pgc_client_types, [descriptor]).

% ------------------------------------------------------------------------------
% bool
% ------------------------------------------------------------------------------

boolsend(true) -> <<1>>;
boolsend(false) -> <<0>>;
boolsend(Value) -> erlang:error(badarg, [Value]).

boolrecv(<<1>>) -> true;
boolrecv(<<0>>) -> false.

% ------------------------------------------------------------------------------
% int2 / int4 / int8
% ------------------------------------------------------------------------------

int2send(Value) when is_integer(Value), Value >= -32768, Value =< 32767 ->
    <<Value:16/signed-integer>>;
int2send(Value) ->
    erlang:error(badarg, [Value]).

int2recv(<<Value:16/signed-integer>>) -> Value.

int4send(Value) when is_integer(Value), Value >= -2147483648, Value =< 2147483647 ->
    <<Value:32/signed-integer>>;
int4send(Value) ->
    erlang:error(badarg, [Value]).

int4recv(<<Value:32/signed-integer>>) -> Value.

int8send(Value) when is_integer(Value), Value >= -9223372036854775808, Value =< 9223372036854775807 ->
    <<Value:64/signed-integer>>;
int8send(Value) ->
    erlang:error(badarg, [Value]).

int8recv(<<Value:64/signed-integer>>) -> Value.

% ------------------------------------------------------------------------------
% float4 / float8
% ------------------------------------------------------------------------------

float4send('NaN') -> <<127, 192, 0, 0>>;
float4send(infinity) -> <<127, 128, 0, 0>>;
float4send('-infinity') -> <<255, 128, 0, 0>>;
float4send(Value) when is_number(Value) -> <<Value:32/signed-float>>;
float4send(Value) -> erlang:error(badarg, [Value]).

float4recv(<<127, 192, 0, 0>>) -> 'NaN';
float4recv(<<127, 128, 0, 0>>) -> infinity;
float4recv(<<255, 128, 0, 0>>) -> '-infinity';
float4recv(<<Value:32/signed-float>>) -> Value.

float8send('NaN') -> <<127, 248, 0, 0, 0, 0, 0, 0>>;
float8send(infinity) -> <<127, 240, 0, 0, 0, 0, 0, 0>>;
float8send('-infinity') -> <<255, 240, 0, 0, 0, 0, 0, 0>>;
float8send(Value) when is_number(Value) -> <<Value:64/signed-float>>;
float8send(Value) -> erlang:error(badarg, [Value]).

float8recv(<<127, 248, 0, 0, 0, 0, 0, 0>>) -> 'NaN';
float8recv(<<127, 240, 0, 0, 0, 0, 0, 0>>) -> infinity;
float8recv(<<255, 240, 0, 0, 0, 0, 0, 0>>) -> '-infinity';
float8recv(<<Value:64/signed-float>>) -> Value.

% ------------------------------------------------------------------------------
% bytea (+ unknown, which is wire-identical)
% ------------------------------------------------------------------------------

byteasend(Value) -> _ = iolist_size(Value), Value.
bytearecv(Data) when is_binary(Data) -> Data.
unknownsend(Value) -> byteasend(Value).
unknownrecv(Data) -> bytearecv(Data).

% ------------------------------------------------------------------------------
% uuid
% ------------------------------------------------------------------------------

uuid_send(<<_:128>> = Uuid) -> Uuid;
uuid_send(Value) -> erlang:error(badarg, [Value]).

uuid_recv(<<_:128>> = Uuid) -> Uuid.

% ------------------------------------------------------------------------------
% text (+ varchar/bpchar/citext, all wire-identical pass-through)
% ------------------------------------------------------------------------------

textsend(Value) -> _ = iolist_size(Value), Value.
textrecv(Data) -> Data.
varcharsend(Value) -> textsend(Value).
varcharrecv(Data) -> textrecv(Data).
bpcharsend(Value) -> textsend(Value).
bpcharrecv(Data) -> textrecv(Data).
citextsend(Value) -> textsend(Value).
citextrecv(Data) -> textrecv(Data).

% ------------------------------------------------------------------------------
% char
% ------------------------------------------------------------------------------

charsend(Char) when is_integer(Char), Char >= 0, Char < 256 -> <<Char>>;
charsend(Value) -> erlang:error(badarg, [Value]).

charrecv(<<Char>>) -> Char.

% ------------------------------------------------------------------------------
% date
% ------------------------------------------------------------------------------

-define(date_epoch, 730485). % calendar:date_to_gregorian_days({2000, 1, 1})

date_send({_, _, _} = Date) ->
    case calendar:valid_date(Date) of
        true -> <<(calendar:date_to_gregorian_days(Date) - ?date_epoch):32/signed-integer>>;
        false -> erlang:error(badarg, [Date])
    end;
date_send(Value) ->
    erlang:error(badarg, [Value]).

date_recv(<<Days:32/signed-integer>>) ->
    calendar:gregorian_days_to_date(Days + ?date_epoch).

% ------------------------------------------------------------------------------
% name
% ------------------------------------------------------------------------------

namesend(Value) when is_binary(Value), byte_size(Value) < 64 -> Value;
namesend(Value) -> erlang:error(badarg, [Value]).

namerecv(Data) -> Data.

% ------------------------------------------------------------------------------
% oid family -- oid/xid/cid/regproc/regprocedure/regoper/regoperator/regclass/regtype all
% share the plain uint32 wire representation.
% ------------------------------------------------------------------------------

oid_send(Value) when is_integer(Value), Value >= 0, Value =< 4294967295 ->
    <<Value:32/integer>>;
oid_send(Value) ->
    erlang:error(badarg, [Value]).

oid_recv(<<Value:32/integer>>) -> Value.

oidsend(Value) -> oid_send(Value).
oidrecv(Data) -> oid_recv(Data).

xidsend(Value) -> oid_send(Value).
xidrecv(Data) -> oid_recv(Data).

cidsend(Value) -> oid_send(Value).
cidrecv(Data) -> oid_recv(Data).

regprocsend(Value) -> oid_send(Value).
regprocrecv(Data) -> oid_recv(Data).

regproceduresend(Value) -> oid_send(Value).
regprocedurerecv(Data) -> oid_recv(Data).

regopersend(Value) -> oid_send(Value).
regoperrecv(Data) -> oid_recv(Data).

regoperatorsend(Value) -> oid_send(Value).
regoperatorrecv(Data) -> oid_recv(Data).

regclasssend(Value) -> oid_send(Value).
regclassrecv(Data) -> oid_recv(Data).

regtypesend(Value) -> oid_send(Value).
regtyperecv(Data) -> oid_recv(Data).

% ------------------------------------------------------------------------------
% tid
% ------------------------------------------------------------------------------

tidsend({Block, Tuple})
        when is_integer(Block), Block >= 0, Block =< 4294967295, is_integer(Tuple), Tuple >= 0, Tuple =< 65535 ->
    <<Block:32/integer, Tuple:16/integer>>;
tidsend(Value) ->
    erlang:error(badarg, [Value]).

tidrecv(<<Block:32/integer, Tuple:16/integer>>) -> {Block, Tuple}.

% ------------------------------------------------------------------------------
% void
% ------------------------------------------------------------------------------

void_send(undefined) -> <<>>;
void_send(Value) -> erlang:error(badarg, [Value]).

void_recv(<<>>) -> undefined.

% ------------------------------------------------------------------------------
% xid8
% ------------------------------------------------------------------------------

xid8send(Value) when is_integer(Value), Value >= 0, Value =< 18_446_744_073_709_551_615 ->
    <<Value:64/integer>>;
xid8send(Value) ->
    erlang:error(badarg, [Value]).

xid8recv(<<Value:64/integer>>) -> Value.

% ------------------------------------------------------------------------------
% bitstring (bit/varbit)
% ------------------------------------------------------------------------------

bitstring_send(Value) when is_bitstring(Value) ->
    Size = bit_size(Value),
    Padding = (8 - (Size rem 8)) rem 8,
    <<Size:32/integer, Value/bitstring, 0:Padding>>;
bitstring_send(Value) ->
    erlang:error(badarg, [Value]).

bitstring_recv(<<Size:32/integer, Bits:Size/bits, _/bits>>) -> Bits.

bit_send(Value) -> bitstring_send(Value).
bit_recv(Data) -> bitstring_recv(Data).

varbit_send(Value) -> bitstring_send(Value).
varbit_recv(Data) -> bitstring_recv(Data).

% ------------------------------------------------------------------------------
% ltree (+ lquery, wire-identical)
% ------------------------------------------------------------------------------

ltree_send(Value) when is_binary(Value) -> <<1:8, Value/binary>>;
ltree_send(Value) -> erlang:error(badarg, [Value]).

ltreesend(Value) -> ltree_send(Value).

ltree_recv(<<1:8, Value/binary>>) -> Value.

ltreerecv(Value) -> ltree_recv(Value).

lquery_send(Value) -> ltree_send(Value).
lquery_recv(Send) -> ltree_recv(Send).

% ------------------------------------------------------------------------------
% hstore
% ------------------------------------------------------------------------------

hstore_send(Value) when is_map(Value) ->
    [<<(map_size(Value)):32/integer>>, maps:fold(fun (Key, Val, Acc) ->
        [hstore_encode_key(Key), hstore_encode_value(Val) | Acc]
    end, [], Value)];
hstore_send(Value) ->
    erlang:error(badarg, [Value]).

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

hstore_recv(<<_Size:32/integer, Payload/binary>>) ->
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

interval_send(Value) when is_map(Value) ->
    MicroSeconds = maps:get(microseconds, Value, 0),
    Days = maps:get(days, Value, 0),
    Months = maps:get(months, Value, 0),
    <<MicroSeconds:64/signed-integer, Days:32/signed-integer, Months:32/signed-integer>>;
interval_send(Value) ->
    erlang:error(badarg, [Value]).

interval_recv(<<MicroSeconds:64/signed-integer, Days:32/signed-integer, Months:32/signed-integer>>) ->
    #{microseconds => MicroSeconds, days => Days, months => Months}.

% ------------------------------------------------------------------------------
% enum
% ------------------------------------------------------------------------------

enum_send(Term) when is_atom(Term) ->
    atom_to_binary(Term);
enum_send(Term) ->
    case unicode:characters_to_binary(Term) of
        Value when is_binary(Value) -> Value;
        _ -> erlang:error(badarg, [Term])
    end.

-doc """
Decode mode comes from this call's `codec => #{enum => #{decode => Mode}}` option (default
`binary`) -- `atom` and `existing_atom` are for callers who know the enum's full value set ahead
of time and want it back as an atom rather than doing that conversion themselves on every row.
""".
enum_recv(Data, _Descriptor, Codec) ->
    case pgc_client_codec:option(enum, decode, binary, Codec) of
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
time_send(Term, _Descriptor, Codec) ->
    <<(time_from_term(Term, time_representation(Codec))):64/signed-integer>>.

time_recv(<<MicroSeconds:64/signed-integer>>, _Descriptor, Codec) ->
    time_to_term(MicroSeconds, time_representation(Codec)).

timetz_send(Term, _Descriptor, Codec) ->
    MicroSeconds = time_from_term(Term, time_representation(Codec)),
    <<MicroSeconds:64/signed-integer, 0:32/signed-integer>>.

timetz_recv(<<MicroSeconds:64/signed-integer, Offset:32/signed-integer>>, _Descriptor, Codec) ->
    time_to_term(MicroSeconds + Offset * 1_000_000, time_representation(Codec)).

time_representation(Codec) ->
    pgc_client_codec:option(time, representation, {system_time, native}, Codec).

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
timestamp_send(infinity, _Descriptor, _Codec) ->
    <<16#7FFFFFFFFFFFFFFF:64/signed-integer>>;
timestamp_send('-infinity', _Descriptor, _Codec) ->
    <<-16#8000000000000000:64/signed-integer>>;
timestamp_send(Term, _Descriptor, Codec) ->
    <<(timestamp_from_term(Term, timestamp_representation(Codec))):64/signed-integer>>.

timestamp_recv(<<16#7FFFFFFFFFFFFFFF:64/signed-integer>>, _Descriptor, _Codec) ->
    infinity;
timestamp_recv(<<-16#8000000000000000:64/signed-integer>>, _Descriptor, _Codec) ->
    '-infinity';
timestamp_recv(<<PGMicroSeconds:64/signed-integer>>, _Descriptor, Codec) ->
    timestamp_to_term(PGMicroSeconds, timestamp_representation(Codec)).

timestamptz_send(Value, Descriptor, Codec) -> timestamp_send(Value, Descriptor, Codec).
timestamptz_recv(Data, Descriptor, Codec) -> timestamp_recv(Data, Descriptor, Codec).

timestamp_representation(Codec) ->
    pgc_client_codec:option(timestamp, representation, {system_time, native}, Codec).

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
json_send(Term, _Descriptor, Codec) ->
    Module = pgc_client_codec:option(json, codec, json, Codec),
    Module:encode(Term).

json_recv(Data, _Descriptor, Codec) ->
    Module = pgc_client_codec:option(json, codec, json, Codec),
    Module:decode(Data).

jsonb_send(Term, Descriptor, Codec) ->
    [1 | json_send(Term, Descriptor, Codec)].

jsonb_recv(<<1, Data/binary>>, Descriptor, Codec) ->
    json_recv(Data, Descriptor, Codec).

% ------------------------------------------------------------------------------
% arrays
% ------------------------------------------------------------------------------

array_send(Values, Descriptor, Codec) ->
    pgc_client_codec_array:encode(Values, Descriptor, Codec).

array_recv(Data, Descriptor, Codec) ->
    pgc_client_codec_array:decode(Data, Descriptor, Codec).

int2vectorsend(Values, Descriptor, Codec) -> array_send(Values, Descriptor, Codec).
int2vectorrecv(Data, Descriptor, Codec) -> array_recv(Data, Descriptor, Codec).

oidvectorsend(Values, Descriptor, Codec) -> array_send(Values, Descriptor, Codec).
oidvectorrecv(Data, Descriptor, Codec) -> array_recv(Data, Descriptor, Codec).


% ------------------------------------------------------------------------------
% records
% ------------------------------------------------------------------------------

record_send(Values, Descriptor, Codec) ->
    pgc_client_codec_record:encode(Values, Descriptor, Codec).

record_recv(Data, Descriptor, Codec) ->
    pgc_client_codec_record:decode(Data, Descriptor, Codec).

% ------------------------------------------------------------------------------
% ranges / multiranges
% ------------------------------------------------------------------------------

range_send(Value, Descriptor, Codec) ->
    pgc_client_codec_range:encode(Value, Descriptor, Codec).

range_recv(Data, Descriptor, Codec) ->
    pgc_client_codec_range:decode(Data, Descriptor, Codec).

multirange_send(Values, Descriptor, Codec) ->
    pgc_client_codec_multirange:encode(Values, Descriptor, Codec).

multirange_recv(Data, Descriptor, Codec) ->
    pgc_client_codec_multirange:decode(Data, Descriptor, Codec).
