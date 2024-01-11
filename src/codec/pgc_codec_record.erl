-module(pgc_codec_record).
-behaviour(pgc_codec).
-export([
    info/1,
    encode/4,
    decode/4
]).

-include("../pgc_type.hrl").


info(_Options) ->
    #{
        encodes => [record_send],
        decodes => [record_recv]
    }.


encode(#pgc_type{name = Name, fields = FieldsDesc}, Input, Options, EncodeFun) ->
    Fields = fields_from_term(Name, FieldsDesc, Input, Options),
    FieldsCount = length(Fields),
    [<<FieldsCount:32/integer>> | encode_fields(EncodeFun, Fields)].


decode(#pgc_type{name = Name, fields = FieldsDesc}, <<_Count:32/integer, Payload/binary>>, Options, DecodeFun) ->
    Fields = decode_fields(DecodeFun, Payload),
    fields_to_term(Name, FieldsDesc, Fields, Options).

% ------------------------------------------------------------------------------
% Encoding
% ------------------------------------------------------------------------------

%% @private
fields_from_term(RecordName, FieldsDesc, Map, Options) when is_map(Map) ->
    [case Map of
        #{Name := Value} -> {Oid, Value};
        #{} -> error(badarg, [RecordName, FieldsDesc, Map, Options])
    end || {Name, Oid} <- FieldsDesc];
fields_from_term(RecordName, FieldsDesc, Value, Options) ->
    error(badarg, [RecordName, FieldsDesc, Value, Options]).


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
fields_to_term(_Options, _Name, undefined, Fields) ->
    % Anonymous record
    lists:foldl(fun ({_Oid, Value}, Acc) ->
        Key = map_size(Acc) + 1,
        Acc#{Key => Value}
    end, #{}, Fields);
fields_to_term(_Options, _Name, FieldsDesc, Fields) ->
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