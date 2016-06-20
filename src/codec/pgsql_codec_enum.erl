-module(pgsql_codec_enum).
-behaviour(pgsql_codec).
-export([
    encodes/1,
    encode/4,
    decodes/1,
    decode/4
]).

encodes(_Opts) ->
    [enum_send].

encode(_Type, Atom, _Codec, _Opts) when is_atom(Atom) ->
    atom_to_binary(Atom, utf8);
encode(_Type, Value, _Codec, _Opts) ->
    error(badarg, [Value]).

decodes(_Opts) ->
    [enum_recv].

decode(_Type, Binary, _Codec, _Opts) ->
    binary_to_atom(Binary, utf8).

