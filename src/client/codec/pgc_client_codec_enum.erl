-module(pgc_client_codec_enum).
-moduledoc false.

-behaviour(pgc_client_codec).
-export([
    names/0,
    encode/3,
    decode/3
]).

names() ->
    [~"enum_send", ~"enum_recv"].

encode(Term, _TypeDescriptor, _Codecs) when is_atom(Term) ->
    atom_to_binary(Term);
encode(Term, TypeDescriptor, Codecs) ->
    case unicode:characters_to_binary(Term) of
        Value when is_binary(Value) -> Value;
        _ -> erlang:error(badarg, [Term, TypeDescriptor, Codecs])
    end.

-doc """
Decode mode comes from this call's `codecs => #{enum => #{decode => Mode}}` option (default
`binary`) -- `atom` and `existing_atom` are for callers who know the enum's full value set ahead
of time and want it back as an atom rather than doing that conversion themselves on every row.
""".
decode(Data, _TypeDescriptor, Codecs) ->
    case maps:get(decode, pgc_client_codecs:options(enum, Codecs), binary) of
        binary -> Data;
        atom -> binary_to_atom(Data);
        existing_atom -> binary_to_existing_atom(Data)
    end.
