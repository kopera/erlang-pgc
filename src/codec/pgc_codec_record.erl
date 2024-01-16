%% @private
-module(pgc_codec_record).
-behaviour(pgc_codec).
-export([
    init/1,
    encode/4,
    decode/4
]).

-include("../types/pgc_type.hrl").


init(Options) ->
    Codec = case Options of
        % #{record := #{codec := C}} when is_atom(C) -> {codec, C};
        % #{record := #{codec := _}} -> erlang:error(badarg, [Options]);
        #{record := map} -> map;
        #{record := _} -> erlang:error(badarg, [Options]);
        #{} -> map
    end,
    Info = #{
        encodes => [record_send],
        decodes => [record_recv]
    },
    {Info, Codec}.


encode(Term, Codec, #pgc_type{namespace = Namespace, name = Name, fields = FieldsDesc}, EncodeFun) ->
    Fields = from_term(Namespace, Name, FieldsDesc, Term, Codec),
    FieldsCount = length(Fields),
    [<<FieldsCount:32/integer>> | encode_fields(EncodeFun, Fields)].


decode(<<_Count:32/integer, Payload/binary>>, Codec, #pgc_type{namespace = Namespace, name = Name, fields = FieldsDesc}, DecodeFun) ->
    Fields = decode_fields(DecodeFun, Payload),
    to_term(Namespace, Name, FieldsDesc, Fields, Codec).


% ------------------------------------------------------------------------------
% Encoding
% ------------------------------------------------------------------------------

%% @private
from_term(_RecordNamespace, _RecordName, FieldsDesc, Map, map) when is_map(Map) ->
    [{Oid, maps:get(FieldName, Map, null)} || {FieldName, Oid} <- FieldsDesc];
% from_term(RecordNamespace, RecordName, FieldsDesc, Term, {codec, CodecModule}) ->
%     FieldNames = [FieldName || {FieldName, _Oid} <- FieldsDesc],
%     Map = CodecModule:encode(RecordNamespace, RecordName, FieldNames, Term),
%     [{Oid, maps:get(FieldName, Map)} || {FieldName, Oid} <- FieldsDesc];
from_term(RecordNamespace, RecordName, FieldsDesc, Value, Codec) ->
    error(badarg, [RecordNamespace, RecordName, FieldsDesc, Value, Codec]).


%% @private
encode_fields(EncodeFun, Fields) ->
    [encode_field(EncodeFun, Oid, FieldValue) || {Oid, FieldValue} <- Fields].


%% @private
encode_field(_EncodeFun, Oid, null) ->
    <<Oid:32/integer, -1:32/signed-integer>>;
encode_field(EncodeFun, Oid, Value) ->
    Encoded = EncodeFun(Oid, Value),
    [<<Oid:32/integer, (iolist_size(Encoded)):32/signed-integer>>, Encoded].


% ------------------------------------------------------------------------------
% Decoding
% ------------------------------------------------------------------------------

%% @private
to_term(_Namespace, _Name, undefined, Fields, map) ->
    % Anonymous record
    lists:foldl(fun ({_Oid, Value}, Acc) ->
        Key = map_size(Acc) + 1,
        Acc#{Key => Value}
    end, #{}, Fields);
to_term(_Namespace, _Name, FieldsDesc, Fields, map) ->
    TupleList = lists:zipwith(fun ({FieldName, Oid}, {Oid, FieldValue}) ->
        {FieldName, FieldValue}
    end, FieldsDesc, Fields),
    maps:from_list(TupleList).


%% @private
decode_fields(DecodeFun, Data) ->
    decode_fields(DecodeFun, Data, []).

%% @private
decode_fields(_DecodeFun, <<>>, Acc) ->
    lists:reverse(Acc);
decode_fields(DecodeFun, <<Oid:32/integer, -1:32/signed-integer, Rest/binary>>, Acc) ->
    decode_fields(DecodeFun, Rest, [{Oid, null} | Acc]);
decode_fields(DecodeFun, <<Oid:32/integer, Size:32/signed-integer, FieldData:Size/binary, Rest/binary>>, Acc) ->
    decode_fields(DecodeFun, Rest, [{Oid, DecodeFun(Oid, FieldData)} | Acc]).