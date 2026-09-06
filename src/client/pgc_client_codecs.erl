-module(pgc_client_codecs).
-moduledoc false.

-export([
    new/1,
    new/2,
    lookup/2,
    codec_for/2,
    with_options/2,
    options/2
]).
-export_type([
    t/0
]).

-record #codecs{
    types :: pgc_client_types:t(),
    modules :: #{binary() => module()},
    options :: #{atom() => term()}
}.
-opaque t() :: #codecs{}.

-spec new(pgc_client_types:t()) -> t().
new(Types) ->
    new(Types, #{}).


-spec new(pgc_client_types:t(), ExtraCodecs) -> t() when
    ExtraCodecs :: #{binary() => module()}.
new(Types, ExtraCodecs) ->
    #codecs{
        types = Types,
        modules = maps:merge(default_codecs(), ExtraCodecs),
        options = #{}
    }.


-spec lookup(pgc_protocol:oid(), t()) -> {ok, pgc_client_types:descriptor()} | error.
lookup(Oid, #codecs{types = Types}) ->
    pgc_client_types:lookup(Oid, Types).


-spec codec_for(Key, t()) -> {ok, module()} | error when
    Key :: binary().
codec_for(Key, #codecs{modules = Modules}) ->
    maps:find(Key, Modules).


-doc """
Layers `Options` (an execute call's `codecs` option, e.g. `#{enum => #{decode => atom}}`) over
whatever this connection's `Codecs` already carries -- currently always `#{}`, until a future
client-level default is added, at which point this is still the only merge point that needs to
change.
""".
-spec with_options(t(), Options) -> t() when
    Options :: #{atom() => term()}.
with_options(#codecs{options = Base} = Codecs, Options) ->
    Codecs#codecs{options = maps:merge(Base, Options)}.


-spec options(Name, t()) -> dynamic() when
    Name :: atom().
options(Name, #codecs{options = Options}) ->
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
        pgc_client_codec_record,
        pgc_client_codec_bitstring,
        pgc_client_codec_char,
        pgc_client_codec_date,
        pgc_client_codec_hstore,
        pgc_client_codec_interval,
        pgc_client_codec_json,
        pgc_client_codec_jsonb,
        pgc_client_codec_ltree,
        pgc_client_codec_name,
        pgc_client_codec_oid,
        pgc_client_codec_tid,
        pgc_client_codec_time,
        pgc_client_codec_timetz,
        pgc_client_codec_timestamp,
        pgc_client_codec_void,
        pgc_client_codec_xid8,
        pgc_client_codec_range,
        pgc_client_codec_multirange
    ].

default_codecs() ->
    maps:from_list([{Name, Module} || Module <- default_codec_modules(), Name <- Module:names()]).
