-module(pgc_client_codec).
-moduledoc false.

-export([
    new/2,
    lookup/2,
    options/2,
    encode/3,
    decode/3
]).
-export_type([
    t/0
]).

-record #codecs{
    types :: pgc_client_types:t(),
    modules :: [module()],
    options :: #{atom() => term()}
}.
-opaque t() :: #codecs{}.

-doc """
`Options` is the same map an `execute` call's `codecs` option carries (`#{enum => #{decode =>
atom}, ...}`) -- an optional `modules` key prepends extra/override codec modules, tried before
`pgc_client_codec_builtin`, so a same-named function wins; everything else becomes the per-codec
options bag (see `options/2`).
""".
-spec new(pgc_client_types:t(), Options) -> t() when
    Options :: #{modules => [module()], atom() => term()}.
new(Types, Options) ->
    ExtraModules = maps:get(modules, Options, []),
    #codecs{
        types = Types,
        modules = ExtraModules ++ [pgc_client_codec_builtin],
        options = maps:remove(modules, Options)
    }.


-spec lookup(pgc_protocol:oid(), t()) -> {ok, pgc_client_types:descriptor()} | error.
lookup(Oid, #codecs{types = Types}) ->
    pgc_client_types:lookup(Oid, Types).


-spec options(Name, t()) -> dynamic() when
    Name :: atom().
options(Name, #codecs{options = Options}) ->
    maps:get(Name, Options, #{}).


% ------------------------------------------------------------------------------
% API
% ------------------------------------------------------------------------------

-spec encode(Value, TypeDescriptor, Codecs) -> iodata() | null when
    Value :: term() | null,
    TypeDescriptor :: pgc_client_types:descriptor(),
    Codecs :: t().
encode(null, _TypeDescriptor, _Codecs) ->
    null;
encode(Value, TypeDescriptor, Codecs) ->
    {Descriptor, Module, Function} = resolve(TypeDescriptor, Codecs, encode),
    Module:Function(Value, Descriptor, Codecs).


-spec decode(Data, TypeDescriptor, Codecs) -> term() when
    Data :: binary() | null,
    TypeDescriptor :: pgc_client_types:descriptor(),
    Codecs :: t().
decode(null, _TypeDescriptor, _Codecs) ->
    null;
decode(Data, TypeDescriptor, Codecs) ->
    {Descriptor, Module, Function} = resolve(TypeDescriptor, Codecs, decode),
    Module:Function(Data, Descriptor, Codecs).


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
    {ok, ParentDescriptor} = lookup(Parent, Codecs),
    resolve(ParentDescriptor, Codecs, Direction);
resolve({_Oid, _Name, _Kind, Recv, Send, _Element, _Parent, _Fields} = Descriptor, #codecs{modules = Modules}, Direction) ->
    Key = case Direction of encode -> Send; decode -> Recv end,
    case find_codec(Key, Modules) of
        {ok, Module, Function} -> {Descriptor, Module, Function};
        error -> erlang:error({codec_missing, Descriptor})
    end.

-doc """
A proc name doubles as the exported function name that implements it (e.g. `int4send` /
`m:pgc_client_codec_builtin.int4send/3`) -- dispatch is just finding which module in the search
list exports it, no separate registry to build or keep in sync.
""".
find_codec(Key, Modules) ->
    try binary_to_existing_atom(Key) of
        Function -> find_module(Function, Modules)
    catch
        error:badarg -> error
    end.

find_module(_Function, []) ->
    error;
find_module(Function, [Module | Rest]) ->
    % function_exported/3 only ever answers against already-loaded code -- ensure_loaded/1
    % first so a module that simply hasn't been called yet (e.g. a caller-supplied override)
    % isn't mistaken for one that doesn't implement this proc name.
    _ = code:ensure_loaded(Module),
    case erlang:function_exported(Module, Function, 3) of
        true -> {ok, Module, Function};
        false -> find_module(Function, Rest)
    end.
