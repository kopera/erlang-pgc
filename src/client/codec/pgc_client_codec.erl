-module(pgc_client_codec).
-moduledoc false.

-export([
    new/2,
    encode/3,
    decode/3
]).
-export([
    option/4
]).
-export_record([
    codec
]).
-export_type([
    t/0
]).

-import_record(pgc_client_types, [descriptor]).

-record #codec{
    types :: pgc_client_types:t(),
    modules :: [module()],
    options :: #{atom() => term()}
}.
-type t() :: #codec{}.


% ------------------------------------------------------------------------------
% API
% ------------------------------------------------------------------------------

-spec new(pgc_client_types:t(), Options) -> t() when
    Options :: #{modules => [module()], atom() => term()}.
new(Types, Options) ->
    ExtraModules = maps:get(modules, Options, []),
    Modules = ExtraModules ++ [pgc_client_codec_builtin],
    lists:foreach(fun code:ensure_loaded/1, Modules),
    #codec{
        types = Types,
        modules = Modules,
        options = maps:remove(modules, Options)
    }.


-spec encode(TypeId, Value, Codec) -> iodata() | null when
    TypeId :: pgc_client_types:id(),
    Value :: term() | null,
    Codec :: t().
encode(_TypeId, null, _Codec) ->
    null;
encode(TypeId, Value, #codec{} = Codec) ->
    {ok, Descriptor} = pgc_client_types:lookup(TypeId, Codec#codec.types),
    case find_encoder(Descriptor, Codec) of
        {ok, Encoder} -> Encoder(Value);
        error -> erlang:error({pgc, {missing_encoder, Descriptor}})
    end.


-spec decode(TypeId, Data, Codec) -> dynamic() when
    TypeId :: pgc_client_types:id(),
    Data :: binary() | null,
    Codec :: t().
decode(_TypeId, null, _Codec) ->
    null;
decode(TypeId, Data, Codec) ->
    {ok, Descriptor} = pgc_client_types:lookup(TypeId, Codec#codec.types),
    case find_decoder(Descriptor, Codec) of
        {ok, Decoder} -> Decoder(Data);
        error -> erlang:error({pgc, {missing_decoder, Descriptor}})
    end.


-spec option(Namespace, Key, Default, #codec{}) -> dynamic() when
    Namespace :: atom(),
    Key :: atom(),
    Default :: dynamic().
option(Namespace, Key, Default, #codec{options = Options}) ->
    case Options of
        #{Namespace := #{Key := Value}} -> Value;
        #{} -> Default
    end.

% ------------------------------------------------------------------------------
% Helpers
% ------------------------------------------------------------------------------

-spec find_encoder(pgc_client_types:descriptor(), #codec{}) -> {ok, fun((term()) -> iodata())} | error.
find_encoder(#descriptor{kind = domain, parent = ParentTypeId}, Codec) when ParentTypeId =/= undefined ->
    {ok, ParentDescriptor} = pgc_client_types:lookup(ParentTypeId, Codec#codec.types),
    find_encoder(ParentDescriptor, Codec);
find_encoder(#descriptor{send = Send} = Descriptor, Codec) ->
    resolve(Send, Descriptor, Codec).

-spec find_decoder(pgc_client_types:descriptor(), #codec{}) -> {ok, fun((term()) -> iodata())} | error.
find_decoder(#descriptor{kind = domain, parent = ParentTypeId}, Codec) when ParentTypeId =/= undefined ->
    {ok, ParentDescriptor} = pgc_client_types:lookup(ParentTypeId, Codec#codec.types),
    find_decoder(ParentDescriptor, Codec);
find_decoder(#descriptor{recv = Recv} = Descriptor, Codec) ->
    resolve(Recv, Descriptor, Codec).


-spec resolve(Name, Descriptor, Codec) -> {ok, CodecFun} | error when
    Name :: unicode:unicode_binary(),
    Descriptor :: pgc_client_types:descriptor(),
    Codec :: #codec{},
    CodecFun :: fun((term()) -> iodata()).
resolve(Name, Descriptor, Codec) ->
    try binary_to_existing_atom(Name) of
        FunctionName ->
            case find_codec_fun(FunctionName, Codec#codec.modules) of
                {ok, Fun} when is_function(Fun, 1) ->
                    {ok, Fun};
                {ok, Fun} when is_function(Fun, 3) ->
                    {ok, fun (Value) -> Fun(Value, Descriptor, Codec) end};
                error ->
                    error
            end
    catch
        error:badarg -> error
    end.


-spec find_codec_fun(Name, Modules) -> {ok, CodecFun} | error when
    Name :: atom(),
    Modules :: [module()],
    CodecFun :: fun((term()) -> iodata()) | fun((term(), pgc_client_types:descriptor(), #codec{}) -> iodata()).
find_codec_fun(_Name, [] = _Modules) ->
    error;
find_codec_fun(Name, [Module | Rest]) ->
    case erlang:function_exported(Module, Name, 3) of
        true ->
            {ok, fun Module:Name/3};
        false ->
            case erlang:function_exported(Module, Name, 1) of
                true -> {ok, fun Module:Name/1};
                false -> find_codec_fun(Name, Rest)
            end
    end.
