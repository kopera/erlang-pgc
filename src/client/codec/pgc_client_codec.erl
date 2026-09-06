-module(pgc_client_codec).
-moduledoc false.

-export([
    encode/3,
    decode/3
]).

-callback names() -> [binary()].
-callback encode(Value, TypeDescriptor, Codecs) -> iodata() when
    Value :: term(),
    TypeDescriptor :: pgc_client_types:descriptor(),
    Codecs :: pgc_client_codecs:t().
-callback decode(Data, TypeDescriptor, Codecs) -> term() when
    Data :: binary(),
    TypeDescriptor :: pgc_client_types:descriptor(),
    Codecs :: pgc_client_codecs:t().

% ------------------------------------------------------------------------------
% API
% ------------------------------------------------------------------------------

-spec encode(Value, TypeDescriptor, Codecs) -> iodata() | null when
    Value :: term() | null,
    TypeDescriptor :: pgc_client_types:descriptor(),
    Codecs :: pgc_client_codecs:t().
encode(null, _TypeDescriptor, _Codecs) ->
    null;
encode(Value, TypeDescriptor, Codecs) ->
    {Descriptor, Module} = resolve(TypeDescriptor, Codecs, encode),
    Module:encode(Value, Descriptor, Codecs).


-spec decode(Data, TypeDescriptor, Codecs) -> term() when
    Data :: binary() | null,
    TypeDescriptor :: pgc_client_types:descriptor(),
    Codecs :: pgc_client_codecs:t().
decode(null, _TypeDescriptor, _Codecs) ->
    null;
decode(Data, TypeDescriptor, Codecs) ->
    {Descriptor, Module} = resolve(TypeDescriptor, Codecs, decode),
    Module:decode(Data, Descriptor, Codecs).


% ------------------------------------------------------------------------------
% Helpers
% ------------------------------------------------------------------------------

-doc """
A domain's wire representation is byte-identical to its base type's -- this is exactly what
Postgres's own generic `domain_recv`/`domain_send` do internally (validate, then defer to the
base type's own function) -- so dispatch resolves straight through to the base type rather than
needing a domain-specific codec.
""".
resolve({_Oid, _Name, domain, _Recv, _Send, _Element, Parent, _Fields}, Codecs, Direction) when Parent =/= undefined ->
    {ok, ParentDescriptor} = pgc_client_codecs:lookup(Parent, Codecs),
    resolve(ParentDescriptor, Codecs, Direction);
resolve({_Oid, _Name, _Kind, Recv, Send, _Element, _Parent, _Fields} = Descriptor, Codecs, Direction) ->
    Key = case Direction of encode -> Send; decode -> Recv end,
    case pgc_client_codecs:codec_for(Key, Codecs) of
        {ok, Module} -> {Descriptor, Module};
        error -> erlang:error({codec_missing, Descriptor})
    end.
