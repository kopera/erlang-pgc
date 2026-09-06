-module(pgc_client_types).
-moduledoc false.

-export([
    new/0,
    new/1,
    add/3,
    has/2,
    lookup/2,
    codec_for/2,
    with_options/2,
    codec_options/2
]).
-export_type([
    t/0,
    descriptor/0
]).

-record #types{
    table :: ets:table(),
    codecs :: #{binary() => module()},
    options :: #{atom() => term()}
}.
-opaque t() :: #types{}.

-type id() :: pgc_protocol:oid().
-type kind() :: base | composite | domain | enum | pseudo | range | multirange | other.
-type descriptor() :: {
    Oid :: id(),
    Name :: binary(),
    Kind :: kind(),
    Recv :: binary(),
    Send :: binary(),
    Element :: id() | undefined,
    Parent :: id() | undefined,
    Fields :: [{binary(), id()}] | undefined
}.

-spec new() -> t().
new() ->
    new(#{}).


-spec new(ExtraCodecs) -> t() when
    ExtraCodecs :: #{binary() => module()}.
new(ExtraCodecs) ->
    #types{
        table = ets:new(?MODULE, [protected, {keypos, 1}]),
        codecs = maps:merge(default_codecs(), ExtraCodecs),
        options = #{}
    }.


-spec add(TypeId, TypeInfo, t()) -> ok when
    TypeId :: id(),
    TypeInfo :: #{
        namespace := binary(),
        name := binary(),
        kind := kind(),
        recv := binary(),
        send := binary(),
        element => id() | undefined,
        parent => id() | undefined,
        fields => [{binary(), id()}] | undefined
    }.
add(Id, #{name := Name, kind := Kind, recv := Recv, send := Send} = Info, #types{table = Table}) ->
    true = ets:insert(Table, {
        Id,
        Name,
        Kind,
        Recv,
        Send,
        maps:get(element, Info, undefined),
        maps:get(parent, Info, undefined),
        maps:get(fields, Info, undefined)
    }),
    ok.


-spec has(id(), t()) -> boolean().
has(Oid, #types{table = Table}) ->
    ets:member(Table, Oid).


-spec lookup(id(), t()) -> {ok, descriptor()} | error.
lookup(Id, #types{table = Table}) ->
    case ets:lookup(Table, Id) of
        [Type] -> {ok, Type};
        [] -> error
    end.


-spec codec_for(Key, t()) -> {ok, module()} | error when
    Key :: binary().
codec_for(Key, #types{codecs = Codecs}) ->
    maps:find(Key, Codecs).


-doc """
Layers `Options` (an execute call's `codecs` option, e.g. `#{enum => #{decode => atom}}`) over
whatever this connection's `Types` already carries -- currently always `#{}`, until a future
client-level default is added, at which point this is still the only merge point that needs to
change.
""".
-spec with_options(t(), Options) -> t() when
    Options :: #{atom() => term()}.
with_options(#types{options = Base} = Types, Options) ->
    Types#types{options = maps:merge(Base, Options)}.


-spec codec_options(Name, t()) -> #{term() => term()} when
    Name :: atom().
codec_options(Name, #types{options = Options}) ->
    maps:get(Name, Options, #{}).


% ------------------------------------------------------------------------------
% Helpers
% ------------------------------------------------------------------------------

default_codec_modules() ->
    [
        pgc_client_codec_bool,
        pgc_client_codec_int2,
        pgc_client_codec_int4,
        pgc_client_codec_int8,
        pgc_client_codec_float4,
        pgc_client_codec_float8,
        pgc_client_codec_bytea,
        pgc_client_codec_uuid,
        pgc_client_codec_text,
        pgc_client_codec_array,
        pgc_client_codec_enum,
        pgc_client_codec_record
    ].

default_codecs() ->
    maps:from_list([{Name, Module} || Module <- default_codec_modules(), Name <- Module:names()]).
