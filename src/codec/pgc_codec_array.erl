-module(pgc_codec_array).
-export([
    init/1,
    encode/4,
    decode/4
]).
-export([
    decode/2
]).

-include("../pgc_type.hrl").


init(_Options) ->
    Info = #{
        encodes => [array_send, int2vectorsend, oidvectorsend],
        decodes => [array_recv, int2vectorrecv, oidvectorrecv]
    },
    {Info, []}.


encode(List, _Options, #pgc_type{element = ElementOid}, EncodeFun) when is_list(List) ->
    {Flags, EncodedElements} = encode_elements(EncodeFun, ElementOid, List),
    [encode_header(ElementOid, Flags, List) | EncodedElements];
encode(Term, Options, Type, EncodeFun) ->
    error(badarg, [Term, Options, Type, EncodeFun]).


decode(Data, _Options, #pgc_type{element = ElementOid}, DecodeFun) ->
    {Lengths, _Flags, ElementOid, Rest} = decode_header(Data),
    Elements = decode_elements(DecodeFun, ElementOid, Rest),
    unflatten(Lengths, Elements).


%% @private
%% Used internally in pgsql_types
decode(DecodeElementFun, Data) ->
    {Lengths, _Flags, ElementOid, Rest} = decode_header(Data),
    Elements = decode_elements(DecodeElementFun, ElementOid, Rest),
    unflatten(Lengths, Elements).


% ------------------------------------------------------------------------------
% Encoding
% ------------------------------------------------------------------------------

%% @private
encode_header(ElementOid, Flags, Value) ->
    Lengths = lengths(Value, []),
    Dims = length(Lengths),
    <<
        Dims:32/signed-integer,
        Flags:32/signed-integer,
        ElementOid:32/signed-integer,
        << <<Length:32/signed-integer, 1:32/signed-integer>> || Length <- Lengths >>/binary
    >>.


%% @private
lengths([], []) ->
    [0];
lengths([], Acc) ->
    lists:reverse(Acc);
lengths([H | _] = Value, Acc) when is_list(H) ->
    lengths(H, [length(Value) | Acc]);
lengths(Value, Acc) ->
    lists:reverse([length(Value) | Acc]).


%% @private
encode_elements(EncodeFun, ElementOid, Values) ->
    encode_elements(EncodeFun, ElementOid, 0, lists:flatten(Values), []).

%% @private
encode_elements(_EncodeFun, _ElementsOid, Flags, [], Acc) ->
    {Flags, lists:reverse(Acc)};
encode_elements(EncodeFun, ElementOid, Flags, [Value | Rest], Acc) ->
    {Flags1, Element} = encode_element(EncodeFun, ElementOid, Flags, Value),
    encode_elements(EncodeFun, ElementOid, Flags1, Rest, [Element | Acc]).


%% @private
encode_element(_EncodeFun, _Oid, Flags, null) ->
    {Flags bor 1, <<-1:32/signed-integer>>};
encode_element(EncodeFun, Oid, Flags, Value) ->
    Encoded = EncodeFun(Oid, Value),
    {Flags, [<<(iolist_size(Encoded)):32/signed-integer>>, Encoded]}.


% ------------------------------------------------------------------------------
% Decoding
% ------------------------------------------------------------------------------

%% @private
%% Convert the 1-d elements list into a multi-dimentional list according to the array lengths.
unflatten([Length | Lengths], Elements) ->
    unflatten(Lengths, split(Length, Elements, []));
unflatten([], [Elements]) ->
    Elements;
unflatten([], []) ->
    [].

%% @private
%% Split a list into sublists of equal size
split(_Length, [], Acc) ->
    lists:reverse(Acc);
split(Length, Elements, Acc) ->
    {Chunk, Rest} = lists:split(Length, Elements),
    split(Length, Rest, [Chunk | Acc]).


%% @private
decode_header(<<Dims:32/signed-integer, Flags:32/signed-integer, ElementOid:32/signed-integer, Rest/binary>>) ->
    {Lengths, Rest1} = decode_lengths(Dims, [], Rest),
    {Lengths, Flags, ElementOid, Rest1}.


%% @private
decode_lengths(0, Lengths, Payload) ->
    {Lengths, Payload};
decode_lengths(Dims, Lengths, <<Length:32/signed-integer, LowerBound:32/signed-integer, Rest/binary>>) ->
    %% We only support arrays with lower bounds = 1
    %% http://postgresql.nabble.com/Disallow-arrays-with-non-standard-lower-bounds-td5786191.html
    1 = LowerBound,
    decode_lengths(Dims - 1, [Length | Lengths], Rest).


%% @private
decode_elements(DecodeElementFun, ElementOid, Payload) ->
    decode_elements(DecodeElementFun, ElementOid, Payload, []).

%% @private
decode_elements(_DecodeElementFun, _ElementsOid, <<>>, Acc) ->
    lists:reverse(Acc);
decode_elements(DecodeElementFun, ElementOid, Data, Acc) ->
    {Element, Rest} = decode_element(DecodeElementFun, ElementOid, Data),
    decode_elements(DecodeElementFun, ElementOid, Rest, [Element | Acc]).


%% @private
decode_element(_DecodeElementFun, _Oid, <<-1:32/signed-integer, Rest/binary>>) ->
    {null, Rest};
decode_element(DecodeElementFun, Oid, <<Size:32/signed-integer, Data:Size/binary, Rest/binary>>) ->
    {DecodeElementFun(Oid, Data), Rest}.