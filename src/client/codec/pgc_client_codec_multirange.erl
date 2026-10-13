-module(pgc_client_codec_multirange).
-moduledoc false.

-export([
    encode/3,
    decode/3
]).

-import_record(pgc_client_codec, [codec]).
-import_record(pgc_client_types, [descriptor]).


-doc """
A multirange's descriptor carries the same `parent` (base scalar type) as its corresponding
range type -- see `refresh_statement_text/0`'s `pg_range` join in `pgc_client`, which matches
on `rngtypid` *or* `rngmultitypid` -- so each element range is encoded/decoded against this
same `Descriptor`.
""".
-spec encode(Ranges, Descriptor, Codec) -> iodata() when
    Ranges :: [pgc_client_codec_range:range()],
    Descriptor :: pgc_client_types:descriptor(),
    Codec :: #codec{}.
encode(Ranges, Descriptor, Codec) when is_list(Ranges) ->
    [<<(length(Ranges)):32/signed-integer>> |
        [encode_range(Range, Descriptor, Codec) || Range <- Ranges]].

encode_range(Range, Descriptor, Codec) ->
    Encoded = pgc_client_codec_range:encode(Range, Descriptor, Codec),
    [<<(iolist_size(Encoded)):32/signed-integer>>, Encoded].


-spec decode(Data, Descriptor, Codec) -> [pgc_client_codec_range:range()] when
    Data :: binary(),
    Descriptor :: pgc_client_types:descriptor(),
    Codec :: #codec{}.
decode(<<Count:32/signed-integer, Data/binary>>, Descriptor, Codec) ->
    decode_ranges(Count, Data, Descriptor, Codec, []).

decode_ranges(0, <<>>, _Descriptor, _Codec, Acc) ->
    lists:reverse(Acc);
decode_ranges(Count, <<Size:32/signed-integer, RangeData:Size/binary, Rest/binary>>, Descriptor, Codec, Acc) ->
    Range = pgc_client_codec_range:decode(RangeData, Descriptor, Codec),
    decode_ranges(Count - 1, Rest, Descriptor, Codec, [Range | Acc]).