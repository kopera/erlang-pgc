-module(pgsql_codec_record).
-behaviour(pgsql_codec).
-export([
    encodes/1,
    encode/4,
    decodes/1,
    decode/4
]).

-include("../../include/types.hrl").

encodes(_Opts) ->
    [<<"record_send">>].

encode(#pgsql_type_info{fields = Fields}, Value, Codec, _Opts) when is_map(Value) ->
    encode_record(Codec, Fields, Value);
encode(_Type, Value, _Codec, _Opts) ->
    error(badarg, [Value]).

decodes(_Opts) ->
    [<<"record_recv">>].

decode(#pgsql_type_info{fields = Fields}, Data, Codec, _Opts) ->
    decode_record(Codec, Fields, Data).

% ==== Encoding

encode_record(Codec, Fields, Map) ->
    Count = length(Fields),
    [<<Count:32/integer>>, lists:map(fun ({Name, Oid}) ->
        Value = maps:get(Name, Map, null),
        encode_field(Codec, Oid, Value)
    end, Fields)].

encode_field(_Codec, Oid, null) ->
    <<Oid:32/integer, -1:32/signed-integer>>;
encode_field(Codec, Oid, Value) ->
    Encoded = pgsql_codec:encode(Oid, Value, Codec),
    [<<Oid:32/integer, (iolist_size(Encoded)):32/signed-integer>>, Encoded].

% ==== Decoding

decode_record(Codec, undefined, <<_Count:32/integer, Payload/binary>>) ->
    decode_fields(Codec, undefined, Payload);
decode_record(Codec, Fields, <<Count:32/integer, Payload/binary>>) when length(Fields) =:= Count ->
    decode_fields(Codec, Fields, Payload).

decode_fields(Codec, Fields, Data) ->
    decode_fields(Codec, Fields, Data, #{}).

decode_fields(_Codec, _, <<>>, Acc) ->
    Acc;
decode_fields(Codec, [{Name, _Oid} | Fields], Data, Acc) ->
    {Value, Rest} = decode_field(Codec, Data),
    decode_fields(Codec, Fields, Rest, maps:put(Name, Value, Acc));
decode_fields(Codec, undefined, Data, Acc) ->
    Name = iolist_to_binary(io_lib:format("f~B", [map_size(Acc) + 1])),
    {Value, Rest} = decode_field(Codec, Data),
    decode_fields(Codec, undefined, Rest, maps:put(Name, Value, Acc)).

decode_field(_Codec, <<_Oid:32/integer, -1:32/signed-integer, Rest/binary>>) ->
    {null, Rest};
decode_field(Codec, <<Oid:32/integer, Size:32/signed-integer, Data:Size/binary, Rest/binary>>) ->
    {pgsql_codec:decode(Oid, Data, Codec), Rest}.
