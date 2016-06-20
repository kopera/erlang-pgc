-module(pgsql_codec_hstore).
-behaviour(pgsql_codec).
-export([
    encodes/1,
    encode/4,
    decodes/1,
    decode/4
]).

encodes(_Opts) ->
    [hstore_send].

encode(_Type, Value, _Codec, _Opts) when is_map(Value) ->
    encode_hstore(Value);
encode(_Type, Value, _Codec, _Opts) ->
    error(badarg, [Value]).

decodes(_Opts) ->
    [hstore_recv].

decode(_Type, Data, _Codec, _Opts) ->
    decode_hstore(Data).


% ==== Encoding

encode_hstore(Map) ->
    [<<(map_size(Map)):32/integer>>, maps:fold(fun (Key, Value, Acc) ->
        [encode_key(Key), encode_value(Value) | Acc]
    end, [], Map)].

encode_key(Key) when is_binary(Key) ->
    <<(byte_size(Key)):32/integer, Key/binary>>;
encode_key(Key) ->
    error(badarg, [Key]).

encode_value(Value) when is_binary(Value) ->
    <<(byte_size(Value)):32/signed-integer, Value/binary>>;
encode_value(null) ->
    <<-1:32/binary>>;
encode_value(Value) ->
    error(badarg, [Value]).

%% ==== Decoding

decode_hstore(<<_Size:32/integer, Payload/binary>>) ->
    decode_hstore(Payload, #{}).

decode_hstore(<<KeyLength:32/integer, Key:KeyLength/binary, -1:32/signed-integer, Rest/binary>>, Acc) ->
    decode_hstore(Rest, maps:put(binary:copy(Key), null, Acc));
decode_hstore(<<KeyLength:32/integer, Key:KeyLength/binary, ValueLength:32/signed-integer, Value:ValueLength/binary, Rest/binary>>, Acc) ->
    decode_hstore(Rest, maps:put(binary:copy(Key), binary:copy(Value), Acc));
decode_hstore(<<>>, Acc) ->
    Acc.
