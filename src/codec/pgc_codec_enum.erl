%% @private
-module(pgc_codec_enum).

-behaviour(pgc_codec).
-export([
    init/1,
    encode/4,
    decode/4
]).

-include("../types/pgc_type.hrl").


init(Options) ->
    Codec = case Options of
        #{enum := #{codec := C}} when is_atom(C) -> {codec, C};
        #{enum := #{codec := _}} -> erlang:error(badarg, [Options]);
        #{enum := atom} -> atom;
        #{enum := existing_atom} -> existing_atom;
        #{enum := attempt_atom} -> attempt_atom;
        #{enum := _} -> erlang:error(badarg, [Options]);
        #{} -> binary
    end,
    Info = #{
        encodes => [enum_send],
        decodes => [enum_recv]
    },
    {Info, Codec}.


encode(Term, atom, _Type, _) when is_atom(Term) ->
    atom_to_binary(Term, utf8);
encode(Term, existing_atom, _Type, _) when is_atom(Term) ->
    atom_to_binary(Term, utf8);
encode(Term, attempt_atom, _Type, _) when is_atom(Term) ->
    atom_to_binary(Term, utf8);
encode(Term, {codec, CodecModule}, #pgc_type{namespace = Namespace, name = Name}, _) ->
    CodecModule:encode(Namespace, Name, Term);
encode(Term, Config, Type, EncodeFun) ->
    case unicode:characters_to_binary(Term) of
        Value when is_binary(Value) ->
            Value;
        _ ->
            error(badarg, [Term, Config, Type, EncodeFun])
    end.


decode(Data, atom, _Type, _) ->
    erlang:binary_to_atom(Data, utf8);
decode(Data, existing_atom, _Type, _) ->
    erlang:binary_to_existing_atom(Data, utf8);
decode(Data, attempt_atom, _Type, _) ->
    try erlang:binary_to_existing_atom(Data, utf8) of
        Term -> Term
    catch
        error:badarg -> Data
    end;
decode(Data, binary, _Type, _) ->
    Data;
decode(Data, {codec, CodecModule}, #pgc_type{namespace = Namespace, name = Name}, _) ->
    CodecModule:decode(Namespace, Name, Data).