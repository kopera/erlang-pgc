-module(pgc_codec_hstore).

-behaviour(pgc_codec).
-export([
    info/1,
    encode/3,
    decode/3
]).


info(_Options) ->
    #{
        encodes => [hstore_send],
        decodes => [hstore_recv]
    }.


encode(_Type, Value, _Options) when is_map(Value) ->
    encode_hstore(Value);
encode(Type, Value, Options) ->
    error(badarg, [Type, Value, Options]).


decode(_Type, Data, _Options) ->
    decode_hstore(Data).


% ------------------------------------------------------------------------------
% Encoding
% ------------------------------------------------------------------------------

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


% ------------------------------------------------------------------------------
% Decoding
% ------------------------------------------------------------------------------

decode_hstore(<<_Size:32/integer, Payload/binary>>) ->
    decode_hstore(Payload, #{}).

decode_hstore(<<KeyLength:32/integer, Key:KeyLength/binary, -1:32/signed-integer, Rest/binary>>, Acc) ->
    decode_hstore(Rest, maps:put(binary:copy(Key), null, Acc));
decode_hstore(<<KeyLength:32/integer, Key:KeyLength/binary, ValueLength:32/signed-integer, Value:ValueLength/binary, Rest/binary>>, Acc) ->
    decode_hstore(Rest, maps:put(binary:copy(Key), binary:copy(Value), Acc));
decode_hstore(<<>>, Acc) ->
    Acc.
