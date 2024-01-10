-module(pgc_codec_enum).

-behaviour(pgc_codec).
-export([
    info/1,
    encode/3,
    decode/3
]).


info(_Options) ->
    #{
        encodes => [enum_send],
        decodes => [enum_recv]
    }.


encode(_Type, Atom, _Options) when is_atom(Atom) ->
    atom_to_binary(Atom, utf8);
encode(_Type, String, _Options) when is_binary(String); is_list(String) ->
    unicode:characters_to_binary(String, utf8);
encode(Type, Value, Options) ->
    error(badarg, [Type, Value, Options]).


decode(_Type, Binary, _Options) ->
    binary_to_atom(Binary, utf8).