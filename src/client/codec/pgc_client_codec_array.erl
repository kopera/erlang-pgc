-module(pgc_client_codec_array).
-moduledoc false.

-export([
    encode/3,
    decode/3
]).

-import_record(pgc_client_codec, [codec]).
-import_record(pgc_client_types, [descriptor]).


%% A list is one dimension, whatever its elements hold: `jsonb[]` elements can be JSON arrays,
%% `text[]` ones iolists. More dimensions are asked for as `{array, Dims, NestedLists}`.
-spec encode(Values, Descriptor, Codec) -> iodata() when
    Values :: list() | {array, pos_integer(), list()},
    Descriptor :: pgc_client_types:descriptor(),
    Codec :: #codec{}.
encode(Array, #descriptor{element = ElementTypeId}, Codec) when is_list(Array) ->
    encode(ElementTypeId, [length(Array)], Array, Codec);
encode({array, Dims, Array}, #descriptor{element = ElementTypeId}, Codec)
        when is_integer(Dims), Dims >= 1, is_list(Array) ->
    encode(ElementTypeId, lengths(Dims, Array), Array, Codec).


-spec decode(Data, Descriptor, Codec) -> list() when
    Data :: binary(),
    Descriptor :: pgc_client_types:descriptor(),
    Codec :: #codec{}.
decode(Data, #descriptor{element = ElementTypeId}, Codec) ->
    DecodeValue = fun(Datum) -> pgc_client_codec:decode(ElementTypeId, Datum, Codec) end,
    decode(Data, DecodeValue).


-spec decode(Data, DecodeValue) -> [Element] when
    Data :: binary(),
    DecodeValue :: fun((binary()) -> Element).
decode(Data, DecodeValue) ->
    {Lengths, Payload} = decode_header(Data),
    Values = decode_values(DecodeValue, Payload),
    unflatten(Lengths, Values).

% ------------------------------------------------------------------------------
% Encoding
% ------------------------------------------------------------------------------

encode_header(ElementTypeId, Flags, Lengths) ->
    Dims = length(Lengths),
    <<
        Dims:32/signed-integer,
        Flags:32/signed-integer,
        ElementTypeId:32/signed-integer,
        << <<Length:32/signed-integer, 1:32/signed-integer>> || Length <- Lengths >>/binary
    >>.

encode(ElementTypeId, Lengths, Array, Codec) ->
    EncodeValue = fun(Value) -> pgc_client_codec:encode(ElementTypeId, Value, Codec) end,
    {Flags, EncodedValues} = encode_values(EncodeValue, values(Lengths, Array)),
    [encode_header(ElementTypeId, Flags, Lengths) | EncodedValues].

%% Each dimension's length is its first list's; `values/2` checks the others.
lengths(1, Array) ->
    [length(Array)];
lengths(Dims, [Nested | _] = Array) when is_list(Nested) ->
    [length(Array) | lengths(Dims - 1, Nested)];
lengths(Dims, Array) ->
    [length(Array) | lists:duplicate(Dims - 1, 0)].

%% Row-major, flattening only the dimensions in `Lengths`: deeper lists are elements.
values([Length], Array) when is_list(Array), length(Array) =:= Length ->
    Array;
values([Length | Lengths], Array) when is_list(Array), length(Array) =:= Length ->
    lists:append([values(Lengths, Nested) || Nested <- Array]);
values(Lengths, _Array) ->
    erlang:error({pgc, {ragged_array, Lengths}}).

encode_values(EncodeValue, Values) ->
    encode_values(EncodeValue, Values, 0, []).

encode_values(_EncodeValue, [], Flags, Acc) ->
    {Flags, lists:reverse(Acc)};
encode_values(EncodeValue, [null | Rest], Flags, Acc) ->
    encode_values(EncodeValue, Rest, Flags bor 1, [<<-1:32/signed-integer>> | Acc]);
encode_values(EncodeValue, [Value | Rest], Flags, Acc) ->
    Encoded = EncodeValue(Value),
    encode_values(EncodeValue, Rest, Flags, [[<<(iolist_size(Encoded)):32/signed-integer>>, Encoded] | Acc]).


% ------------------------------------------------------------------------------
% Decoding
% ------------------------------------------------------------------------------

decode_header(<<Dims:32/signed-integer, _Flags:32/signed-integer, _ElementOid:32/signed-integer, Rest/binary>>) ->
    decode_lengths(Dims, [], Rest).


decode_lengths(0, Lengths, Payload) ->
    {Lengths, Payload};
decode_lengths(Dims, Lengths, <<Length:32/signed-integer, LowerBound:32/signed-integer, Rest/binary>>) ->
    1 = LowerBound,
    decode_lengths(Dims - 1, [Length | Lengths], Rest).


decode_values(_DecodeValue, <<>>) ->
    [];
decode_values(DecodeValue, <<-1:32/signed-integer, Rest/binary>>) ->
    [null | decode_values(DecodeValue, Rest)];
decode_values(DecodeValue, <<Size:32/signed-integer, Data:Size/binary, Rest/binary>>) ->
    [DecodeValue(Data) | decode_values(DecodeValue, Rest)].


unflatten([Length | Lengths], Elements) ->
    unflatten(Lengths, split(Length, Elements));
unflatten([], [Elements]) ->
    Elements;
unflatten([], []) ->
    [].


-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

shape_test() ->
    Shape = fun (Dims, Array) -> values(lengths(Dims, Array), Array) end,
    ?assertEqual([], Shape(1, [])),
    ?assertEqual([], Shape(2, [])),
    ?assertEqual([1, 2, 3, 4], Shape(2, [[1, 2], [3, 4]])),
    %% Lists below the dimensions asked for are elements.
    ?assertEqual([[1, 2], [3, 4]], Shape(1, [[1, 2], [3, 4]])),
    ?assertEqual([[1], [2]], Shape(2, [[[1]], [[2]]])),
    ?assertError({pgc, {ragged_array, _}}, Shape(2, [[1, 2], [3]])),
    ?assertError({pgc, {ragged_array, _}}, Shape(2, [[1], 2])),
    ?assertError({pgc, {ragged_array, _}}, Shape(2, [1, 2])).
-endif.


-doc "Split a list into sublists of equal size.".
split(_Length, []) ->
    [];
split(Length, Elements) ->
    {Chunk, Rest} = lists:split(Length, Elements),
    [Chunk | split(Length, Rest)].
