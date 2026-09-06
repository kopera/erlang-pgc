-module(pgc_client_codec_multirange).
-moduledoc false.

-behaviour(pgc_client_codec).
-export([
    names/0,
    encode/3,
    decode/3
]).

names() ->
    [~"multirange_send", ~"multirange_recv"].

encode(Ranges, {_Oid, _Name, _Kind, _Recv, _Send, _Element, Parent, _Fields}, Codecs) when is_list(Ranges) ->
    {ok, ElementDescriptor} = pgc_client_codecs:lookup(Parent, Codecs),
    [<<(length(Ranges)):32/signed-integer>> |
        [encode_range(Range, ElementDescriptor, Codecs) || Range <- Ranges]];
encode(Value, TypeDescriptor, Codecs) ->
    erlang:error(badarg, [Value, TypeDescriptor, Codecs]).

encode_range(Range, ElementDescriptor, Codecs) ->
    Encoded = pgc_client_codec_range:encode_range(Range, ElementDescriptor, Codecs),
    [<<(iolist_size(Encoded)):32/signed-integer>>, Encoded].

decode(<<Count:32/signed-integer, Data/binary>>, {_Oid, _Name, _Kind, _Recv, _Send, _Element, Parent, _Fields}, Codecs) ->
    {ok, ElementDescriptor} = pgc_client_codecs:lookup(Parent, Codecs),
    decode_ranges(Count, Data, ElementDescriptor, Codecs, []).

decode_ranges(0, <<>>, _ElementDescriptor, _Codecs, Acc) ->
    lists:reverse(Acc);
decode_ranges(Count, <<Size:32/signed-integer, RangeData:Size/binary, Rest/binary>>, ElementDescriptor, Codecs, Acc) ->
    {Range, <<>>} = pgc_client_codec_range:decode_range(RangeData, ElementDescriptor, Codecs),
    decode_ranges(Count - 1, Rest, ElementDescriptor, Codecs, [Range | Acc]).
