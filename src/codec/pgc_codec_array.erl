-module(pgc_codec_array).
-export([
    decode/2
]).


%% @private
%% Used internally in pgsql_types
decode(DecodeElementFun, Data) ->
    {Lengths, _Flags, ElementOid, Rest} = decode_header(Data),
    Elements = decode_elements(DecodeElementFun, ElementOid, Rest),
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
decode_elements(DecodeElementFun, ElementOid, Payload) ->
    decode_elements(DecodeElementFun, ElementOid, Payload, []).

decode_elements(_DecodeElementFun, _ElementsOid, <<>>, Acc) ->
    lists:reverse(Acc);
decode_elements(DecodeElementFun, ElementOid, Data, Acc) ->
    {Element, Rest} = decode_element(DecodeElementFun, ElementOid, Data),
    decode_elements(DecodeElementFun, ElementOid, Rest, [Element | Acc]).

decode_element(_DecodeElementFun, _Oid, <<-1:32/signed-integer, Rest/binary>>) ->
    {null, Rest};
decode_element(DecodeElementFun, Oid, <<Size:32/signed-integer, Data:Size/binary, Rest/binary>>) ->
    {DecodeElementFun(Oid, Data), Rest}.