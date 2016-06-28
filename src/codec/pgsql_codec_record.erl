-module(pgsql_codec_record).
-behaviour(pgsql_codec).
-export([
    init/2,
    encodes/1,
    encode/4,
    decodes/1,
    decode/4
]).

-include("../../include/types.hrl").

init(_, Opts) ->
    maps:get(record_format, Opts, tuple).

encodes(_Opts) ->
    [record_send].

encode(#pgsql_type_info{fields = FieldsDesc}, Value, Codec, Format) ->
    Fields = input(Format, FieldsDesc, Value),
    encode_record(Codec, Fields).

decodes(_Opts) ->
    [record_recv].

decode(#pgsql_type_info{name = Name, fields = Fields}, Data, Codec, Format) ->
    output(Format, Name, Fields, decode_record(Codec, Data)).


% Internals

input(tuple, Fields, Tuple) when is_tuple(Tuple), tuple_size(Tuple) =:= length(Fields) ->
    lists:zipwith(fun ({_Name, Oid}, Value) -> {Oid, Value} end, Fields, tuple_to_list(Tuple));
input(record, Fields, Record) when is_tuple(Record), (tuple_size(Record) - 1) =:= length(Fields) ->
    lists:zipwith(fun ({_Name, Oid}, Value) -> {Oid, Value} end, Fields, tl(tuple_to_list(Record)));
input(_, Fields, Map) when is_map(Map) ->
    [{Oid, maps:get(Name, Map, null)} || {Name, Oid} <- Fields];
input(_, _, Value) ->
    error(badarg, [Value]).

output(tuple, _, _, Values) ->
    list_to_tuple(Values);
output(record, Name, _, Values) ->
    list_to_tuple([Name, Values]);
output(map, _Name, undefined, Values) ->
    lists:foldl(fun (Value, Acc) ->
        Name = iolist_to_binary(io_lib:format("f~B", [map_size(Acc) + 1])),
        maps:put(Name, Value, Acc)
    end, #{}, Values);
output(map, _Name, Fields, Values) ->
    maps:from_list(lists:zipwith(fun ({Name, _}, Value) -> {Name, Value} end, Fields, Values)).


% ==== Encoding

encode_record(Codec, Fields) ->
    Count = length(Fields),
    [<<Count:32/integer>>, lists:map(fun ({Oid, Value}) ->
        encode_field(Codec, Oid, Value)
    end, Fields)].

encode_field(_Codec, Oid, null) ->
    <<Oid:32/integer, -1:32/signed-integer>>;
encode_field(Codec, Oid, Value) ->
    Encoded = pgsql_codec:encode(Oid, Value, Codec),
    [<<Oid:32/integer, (iolist_size(Encoded)):32/signed-integer>>, Encoded].

% ==== Decoding

decode_record(Codec, <<_Count:32/integer, Payload/binary>>) ->
    decode_fields(Codec, Payload).

decode_fields(Codec, Data) ->
    decode_fields(Codec, Data, []).

decode_fields(_Codec, <<>>, Acc) ->
    lists:reverse(Acc);
decode_fields(Codec, Data, Acc) ->
    {Value, Rest} = decode_field(Codec, Data),
    decode_fields(Codec, Rest, [Value | Acc]).

decode_field(_Codec, <<_Oid:32/integer, -1:32/signed-integer, Rest/binary>>) ->
    {null, Rest};
decode_field(Codec, <<Oid:32/integer, Size:32/signed-integer, Data:Size/binary, Rest/binary>>) ->
    {pgsql_codec:decode(Oid, Data, Codec), Rest}.
