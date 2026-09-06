-module(pgc_client_codec_array).
-moduledoc false.

-behaviour(pgc_client_codec).
-export([
    names/0,
    encode/3,
    decode/3
]).

names() ->
    [~"array_send", ~"array_recv", ~"int2vectorsend", ~"int2vectorrecv", ~"oidvectorsend", ~"oidvectorrecv"].

encode(List, {_Oid, _Name, _Kind, _Recv, _Send, ElementOid, _Parent, _Fields}, Types) when is_list(List) ->
    {Flags, EncodedElements} = encode_elements(ElementOid, Types, List),
    [encode_header(ElementOid, Flags, List) | EncodedElements];
encode(Value, TypeDescriptor, Types) ->
    erlang:error(badarg, [Value, TypeDescriptor, Types]).


decode(Data, {_Oid, _Name, _Kind, _Recv, _Send, ElementOid, _Parent, _Fields}, Types) ->
    {Lengths, _Flags, ElementOid, Rest} = decode_header(Data),
    Elements = decode_elements(ElementOid, Types, Rest),
    unflatten(Lengths, Elements).


% ------------------------------------------------------------------------------
% Encoding
% ------------------------------------------------------------------------------

encode_header(ElementOid, Flags, Value) ->
    Lengths = lengths(Value, []),
    Dims = length(Lengths),
    <<
        Dims:32/signed-integer,
        Flags:32/signed-integer,
        ElementOid:32/signed-integer,
        << <<Length:32/signed-integer, 1:32/signed-integer>> || Length <- Lengths >>/binary
    >>.

lengths([], []) ->
    [0];
lengths([], Acc) ->
    lists:reverse(Acc);
lengths([H | _] = Value, Acc) when is_list(H) ->
    lengths(H, [length(Value) | Acc]);
lengths(Value, Acc) ->
    lists:reverse([length(Value) | Acc]).

encode_elements(ElementOid, Types, Values) ->
    {ok, ElementDescriptor} = pgc_client_types:lookup(ElementOid, Types),
    encode_elements(ElementDescriptor, Types, 0, lists:flatten(Values), []).

encode_elements(_ElementDescriptor, _Types, Flags, [], Acc) ->
    {Flags, lists:reverse(Acc)};
encode_elements(ElementDescriptor, Types, Flags, [Value | Rest], Acc) ->
    {Flags1, Element} = encode_element(ElementDescriptor, Types, Flags, Value),
    encode_elements(ElementDescriptor, Types, Flags1, Rest, [Element | Acc]).

encode_element(_ElementDescriptor, _Types, Flags, null) ->
    {Flags bor 1, <<-1:32/signed-integer>>};
encode_element(ElementDescriptor, Types, Flags, Value) ->
    Encoded = pgc_client_codec:encode(Value, ElementDescriptor, Types),
    {Flags, [<<(iolist_size(Encoded)):32/signed-integer>>, Encoded]}.


% ------------------------------------------------------------------------------
% Decoding
% ------------------------------------------------------------------------------

-doc "Convert the 1-d elements list into a multi-dimensional list according to the array lengths.".
unflatten([Length | Lengths], Elements) ->
    unflatten(Lengths, split(Length, Elements, []));
unflatten([], [Elements]) ->
    Elements;
unflatten([], []) ->
    [].

-doc "Split a list into sublists of equal size.".
split(_Length, [], Acc) ->
    lists:reverse(Acc);
split(Length, Elements, Acc) ->
    {Chunk, Rest} = lists:split(Length, Elements),
    split(Length, Rest, [Chunk | Acc]).

decode_header(<<Dims:32/signed-integer, Flags:32/signed-integer, ElementOid:32/signed-integer, Rest/binary>>) ->
    {Lengths, Rest1} = decode_lengths(Dims, [], Rest),
    {Lengths, Flags, ElementOid, Rest1}.

decode_lengths(0, Lengths, Payload) ->
    {Lengths, Payload};
decode_lengths(Dims, Lengths, <<Length:32/signed-integer, LowerBound:32/signed-integer, Rest/binary>>) ->
    % Only arrays with a lower bound of 1 are supported.
    % http://postgresql.nabble.com/Disallow-arrays-with-non-standard-lower-bounds-td5786191.html
    1 = LowerBound,
    decode_lengths(Dims - 1, [Length | Lengths], Rest).

decode_elements(ElementOid, Types, Payload) ->
    {ok, ElementDescriptor} = pgc_client_types:lookup(ElementOid, Types),
    decode_elements(ElementDescriptor, Types, Payload, []).

decode_elements(_ElementDescriptor, _Types, <<>>, Acc) ->
    lists:reverse(Acc);
decode_elements(ElementDescriptor, Types, Data, Acc) ->
    {Element, Rest} = decode_element(ElementDescriptor, Types, Data),
    decode_elements(ElementDescriptor, Types, Rest, [Element | Acc]).

decode_element(_ElementDescriptor, _Types, <<-1:32/signed-integer, Rest/binary>>) ->
    {null, Rest};
decode_element(ElementDescriptor, Types, <<Size:32/signed-integer, Data:Size/binary, Rest/binary>>) ->
    {pgc_client_codec:decode(Data, ElementDescriptor, Types), Rest}.
