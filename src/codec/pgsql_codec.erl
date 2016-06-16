-module(pgsql_codec).
-export([
    new/3,
    new/2,
    update_parameters/2,
    update_types/2,
    has_types/2,
    encode/3,
    decode/3
]).
-export_type([
    codec/0
]).


-callback init(params(), options()) -> state().
-callback encodes(state()) -> [binary()].
-callback encode(type(), Value :: any(), codec(), state()) -> iodata().
-callback decodes(state()) -> [binary()].
-callback decode(type(), binary(), codec(), state()) -> any().
-optional_callbacks([init/2]).


-record(codec, {
    parameters :: map(),
    options :: map(),
    states = #{} :: #{module() => state()},
    types = ordsets:new() :: ordsets:ordset(pgsql_types:oid()),
    encoders = #{} :: #{pgsql_types:oid () => {module(), type()}},
    decoders = #{} :: #{pgsql_types:oid () => {module(), type()}}
}).
-type oid() :: pgsql_types:oid().
-type type() :: pgsql_types:type().
-type types() :: [type()].
-type params() :: map().
-type options() :: map().
-type state() :: any().
-opaque codec() :: codec().

-include("../../include/types.hrl").


-spec new(params(), options(), [module()]) -> codec().
new(Parameters, Options, Modules) ->
    #codec{
        parameters = Parameters,
        options = Options,
        states = maps:from_list([{Module, mod_init(Module, Parameters, Options)} || Module <- Modules])
    }.

new(Parameters, Options) ->
    new(Parameters, Options, [
        pgsql_codec_array,
        pgsql_codec_binary,
        pgsql_codec_bool,
        pgsql_codec_date,
        pgsql_codec_enum,
        pgsql_codec_float4,
        pgsql_codec_float8,
        pgsql_codec_int2,
        pgsql_codec_int4,
        pgsql_codec_int8,
        pgsql_codec_interval,
        pgsql_codec_oid,
        pgsql_codec_text,
        pgsql_codec_uuid,
        pgsql_codec_void
    ]).

mod_init(Module, Parameters, Options) ->
    case erlang:function_exported(Module, init, 2) of
        true ->
            Module:init(Parameters, Options);
        false ->
            Options
    end.

-spec update_parameters(params(), codec()) -> codec().
update_parameters(Parameters, #codec{options = Options, states = Codecs} = Codec) ->
    Codec#codec{
        parameters = Parameters,
        states = maps:map(fun (Module, _) -> mod_init(Module, Parameters, Options) end, Codecs)
    }.

-spec update_types(types(), codec()) -> codec().
update_types(Types, #codec{states = States} = Codec) ->
    StatesList = maps:to_list(States),
    Codec#codec{
        types = ordsets:from_list([Oid || #pgsql_type_info{oid = Oid} <- Types]),
        encoders = lists:foldl(fun (#pgsql_type_info{oid = Oid, send = Send} = Type, Acc) ->
            case find_encoder(Send, StatesList) of
                {ok, Encoder} -> maps:put(Oid, {Encoder, Type}, Acc);
                error -> Acc
            end
        end, #{}, Types),
        decoders = lists:foldl(fun (#pgsql_type_info{oid = Oid, recv = Recv} = Type, Acc) ->
            case find_decoder(Recv, StatesList) of
                {ok, Decoder} -> maps:put(Oid, {Decoder, Type}, Acc);
                error -> Acc
            end
        end, #{}, Types)
    }.

-spec has_types(types(), codec()) -> boolean().
has_types(Types, #codec{types = Known}) ->
    ordsets:is_subset(Types, Known).

find_encoder(_, []) ->
    error;
find_encoder(Send, [{Module, State} | Codecs]) ->
    case lists:member(Send, Module:encodes(State)) of
        true -> {ok, Module};
        false -> find_encoder(Send, Codecs)
    end.

find_decoder(_, []) ->
    error;
find_decoder(Recv, [{Module, State} | Codecs]) ->
    case lists:member(Recv, Module:decodes(State)) of
        true -> {ok, Module};
        false -> find_decoder(Recv, Codecs)
    end.

-spec encode(oid(), any(), codec()) -> binary().
encode(Oid, Value, #codec{states = States, encoders = Encoders} = Codec) ->
    case maps:find(Oid, Encoders) of
        {ok, {Module, Type}} ->
            Module:encode(Type, Value, Codec, maps:get(Module, States));
        error ->
            exit({no_encoder, Oid})
    end.

-spec decode(oid(), binary(), codec()) -> binary().
decode(Oid, Value, #codec{states = States, decoders = Decoders} = Codec) ->
    case maps:find(Oid, Decoders) of
        {ok, {Module, Type}} ->
            Module:decode(Type, Value, Codec, maps:get(Module, States));
        error ->
            exit({no_decoder, Oid})
    end.
