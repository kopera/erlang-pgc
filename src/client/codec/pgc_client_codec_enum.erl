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

encode(Term, _TypeDescriptor, _Types) when is_atom(Term) ->
    atom_to_binary(Term);
encode(Term, TypeDescriptor, Types) ->
    case unicode:characters_to_binary(Term) of
        Value when is_binary(Value) -> Value;
        _ -> erlang:error(badarg, [Term, TypeDescriptor, Types])
    end.

-doc """
Decode mode comes from this call's `codecs => #{enum => #{decode => Mode}}` option (default
`binary`) -- `atom` and `existing_atom` are for callers who know the enum's full value set ahead
of time and want it back as an atom rather than doing that conversion themselves on every row.
""".
decode(Data, _TypeDescriptor, Types) ->
    case maps:get(decode, pgc_client_types:codec_options(enum, Types), binary) of
        binary -> Data;
        atom -> binary_to_atom(Data);
        existing_atom -> binary_to_existing_atom(Data)
    end.
