-module(pgc_client_codec).
-moduledoc false.

-export([
    encode/3,
    decode/3
]).

-callback names() -> [binary()].
-callback encode(Value, TypeDescriptor, Types) -> iodata() when
    Value :: term(),
    TypeDescriptor :: pgc_client_types:descriptor(),
    Types :: pgc_client_types:t().
-callback decode(Data, TypeDescriptor, Types) -> term() when
    Data :: binary(),
    TypeDescriptor :: pgc_client_types:descriptor(),
    Types :: pgc_client_types:t().

% ------------------------------------------------------------------------------
% API
% ------------------------------------------------------------------------------

-spec encode(Value, TypeDescriptor, Types) -> iodata() | null when
    Value :: term() | null,
    TypeDescriptor :: pgc_client_types:descriptor(),
    Types :: pgc_client_types:t().
encode(null, _TypeDescriptor, _Types) ->
    null;
encode(Value, TypeDescriptor, Types) ->
    {Descriptor, Module} = resolve(TypeDescriptor, Types, encode),
    Module:encode(Value, Descriptor, Types).


-spec decode(Data, TypeDescriptor, Types) -> term() when
    Data :: binary() | null,
    TypeDescriptor :: pgc_client_types:descriptor(),
    Types :: pgc_client_types:t().
decode(null, _TypeDescriptor, _Types) ->
    null;
decode(Data, TypeDescriptor, Types) ->
    {Descriptor, Module} = resolve(TypeDescriptor, Types, decode),
    Module:decode(Data, Descriptor, Types).


% ------------------------------------------------------------------------------
% Helpers
% ------------------------------------------------------------------------------

-doc """
A domain's wire representation is byte-identical to its base type's -- this is exactly what
Postgres's own generic `domain_recv`/`domain_send` do internally (validate, then defer to the
base type's own function) -- so dispatch resolves straight through to the base type rather than
needing a domain-specific codec.
""".
resolve({_Oid, _Name, domain, _Recv, _Send, _Element, Parent, _Fields}, Types, Direction) when Parent =/= undefined ->
    {ok, ParentDescriptor} = pgc_client_types:lookup(Parent, Types),
    resolve(ParentDescriptor, Types, Direction);
resolve({_Oid, _Name, _Kind, Recv, Send, _Element, _Parent, _Fields} = Descriptor, Types, Direction) ->
    Key = case Direction of encode -> Send; decode -> Recv end,
    case pgc_client_types:codec_for(Key, Types) of
        {ok, Module} -> {Descriptor, Module};
        error -> erlang:error({codec_missing, Descriptor})
    end.
