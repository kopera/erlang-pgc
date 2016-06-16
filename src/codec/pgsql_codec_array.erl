-module(pgsql_codec_array).

-behaviour(pgsql_codec).
-export([
    encodes/1,
    encode/4,
    decodes/1,
    decode/4
]).

-export([
    decode/2
]).

-include("../../include/types.hrl").

encodes(_Opts) ->
    [<<"array_send">>, <<"int2vectorsend">>, <<"oidvectorsend">>].

encode(#pgsql_type_info{element = ElementOid}, Value, Codec, _Opts) when is_list(Value) ->
    encode_array(ElementOid, Codec, Value);
encode(_Type, Value, _Codec, _Opts) ->
    error(badarg, [Value]).

decodes(_Opts) ->
    [<<"array_recv">>, <<"int2vectorrecv">>, <<"oidvectorrecv">>].

decode(#pgsql_type_info{element = ElementOid}, Data, Codec, _Opts) ->
    decode_array(fun (Oid, Element) when Oid =:= ElementOid ->
        pgsql_codec:decode(Oid, Element, Codec)
    end, Data).

%% @private
%% Used internally in pgsql_types
decode(Fun, Data) ->
    decode_array(Fun, Data).

% ==== Encoding

encode_array(ElementOid, Codec, Value) ->
    {Flags, Elements} = encode_elements(ElementOid, Codec, Value),
    [encode_header(Value, Flags, ElementOid), Elements].

% header
encode_header(Value, Flags, ElementOid) ->
    Lengths = lengths(Value, []),
    Dims = length(Lengths),
    <<
        Dims:32/signed-integer,
        Flags:32/signed-integer,
        ElementOid:32/signed-integer,
        << <<Length:32/signed-integer, 1:32/signed-integer>> || Length <- Lengths >>
    >>.

lengths([], []) ->
    [0];
lengths([], Acc) ->
    lists:reverse(Acc);
lengths([H | _] = Value, Acc) when is_list(H) ->
    lengths(H, [length(Value) | Acc]);
lengths(Value, Acc) ->
    lists:reverse([length(Value) | Acc]).

% elements
encode_elements(ElementOid, Codec, Elements) ->
    encode_elements(ElementOid, Codec, 0, lists:flatten(Elements), []).

encode_elements(_ElementsOid, _Codec, Flags, [], Acc) ->
    {Flags, lists:reverse(Acc)};
encode_elements(ElementOid, Codec, Flags, [Value | Rest], Acc) ->
    {Flags1, Element} = encode_element(ElementOid, Codec, Flags, Value),
    encode_elements(Codec, ElementOid, Flags1, Rest, [Element | Acc]).

encode_element(_Oid, _Codec, Flags, null) ->
    {Flags bor 1, <<-1:32/signed-integer>>};
encode_element(Oid, Codec, Flags, Value) ->
    Encoded = pgsql_codec:encode(Oid, Value, Codec),
    {Flags, [<<(iolist_size(Encoded)):32/signed-integer>>, Encoded]}.

% ==== Decoding

decode_array(ElementDecoder, Data) ->
    {Lengths, _Flags, ElementOid, Rest} = decode_header(Data),
    Elements = decode_elements(ElementDecoder, ElementOid, Rest),
    unflatten(Lengths, Elements).

%% Convert the 1-d elements list into a multi-dimentional list according to the array lengths.
unflatten([Length | Lengths], Elements) ->
    unflatten(Lengths, split(Length, Elements, []));
unflatten([], [Elements]) ->
    Elements;
unflatten([], []) ->
    [].

%% Split a list into sublists of equal size
split(_Length, [], Acc) ->
    lists:reverse(Acc);
split(Length, Elements, Acc) ->
    {Chunk, Rest} = lists:split(Length, Elements),
    split(Length, Rest, [Chunk | Acc]).

% header
decode_header(<<Dims:32/signed-integer, Flags:32/signed-integer, ElementOid:32/signed-integer, Rest/binary>>) ->
    {Lengths, Rest1} = decode_lengths(Dims, [], Rest),
    {Lengths, Flags, ElementOid, Rest1}.

decode_lengths(0, Lengths, Payload) ->
    {Lengths, Payload};
decode_lengths(Dims, Lengths, <<Length:32/signed-integer, LowerBound:32/signed-integer, Rest/binary>>) ->
    %% We only support arrays with lower bounds = 1
    %% http://postgresql.nabble.com/Disallow-arrays-with-non-standard-lower-bounds-td5786191.html
    1 = LowerBound,
    decode_lengths(Dims - 1, [Length | Lengths], Rest).

% elements
decode_elements(ElementDecoder, ElementOid, Payload) ->
    decode_elements(ElementDecoder, ElementOid, Payload, []).

decode_elements(_ElementDecoder, _ElementsOid, <<>>, Acc) ->
    lists:reverse(Acc);
decode_elements(ElementDecoder, ElementOid, Data, Acc) ->
    {Element, Rest} = decode_element(ElementDecoder, ElementOid, Data),
    decode_elements(ElementDecoder, ElementOid, Rest, [Element | Acc]).

decode_element(_Decoder, _Oid, <<-1:32/signed-integer, Rest/binary>>) ->
    {null, Rest};
decode_element(Decoder, Oid, <<Size:32/signed-integer, Data:Size/binary, Rest/binary>>) ->
    {Decoder(Oid, Data), Rest}.
