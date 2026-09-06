-module(pgc_client_codec_hstore).
-moduledoc false.

-behaviour(pgc_client_codec).
-export([
    names/0,
    encode/3,
    decode/3
]).

names() ->
    [~"hstore_send", ~"hstore_recv"].

encode(Value, _TypeDescriptor, _Codecs) when is_map(Value) ->
    [<<(map_size(Value)):32/integer>>, maps:fold(fun (Key, Val, Acc) ->
        [encode_key(Key), encode_value(Val) | Acc]
    end, [], Value)];
encode(Value, TypeDescriptor, Codecs) ->
    erlang:error(badarg, [Value, TypeDescriptor, Codecs]).

encode_key(Key) when is_binary(Key) ->
    <<(byte_size(Key)):32/integer, Key/binary>>;
encode_key(Key) ->
    erlang:error(badarg, [Key]).

encode_value(null) ->
    <<-1:32/signed-integer>>;
encode_value(Value) when is_binary(Value) ->
    <<(byte_size(Value)):32/signed-integer, Value/binary>>;
encode_value(Value) ->
    erlang:error(badarg, [Value]).


decode(<<_Size:32/integer, Payload/binary>>, _TypeDescriptor, _Codecs) ->
    decode_pairs(Payload, #{}).

decode_pairs(<<KeyLength:32/integer, Key:KeyLength/binary, -1:32/signed-integer, Rest/binary>>, Acc) ->
    decode_pairs(Rest, Acc#{Key => null});
decode_pairs(<<KeyLength:32/integer, Key:KeyLength/binary, ValueLength:32/signed-integer, Value:ValueLength/binary, Rest/binary>>, Acc) ->
    decode_pairs(Rest, Acc#{Key => Value});
decode_pairs(<<>>, Acc) ->
    Acc.
