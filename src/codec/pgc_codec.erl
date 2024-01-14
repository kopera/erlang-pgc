-module(pgc_codec).


-callback init(pgc_connection:parameters(), options()) -> CodecConfig :: term().
-callback info(CodecConfig :: term()) -> #{encodes := [atom()], decodes := [atom()]}.
-callback encode(pgc_type:t(), Value :: term(), CodecConfig :: term()) -> Data :: iodata().
-callback encode(pgc_type:t(), Value :: term(), CodecConfig :: term(), fun((pgc_type:oid(), term()) -> iodata())) -> Data :: iodata().
-callback decode(pgc_type:t(), Data :: binary(), CodecConfig :: term()) -> Value :: term().
-callback decode(pgc_type:t(), Data :: binary(), CodecConfig :: term(), fun((pgc_type:oid(), term()) -> term())) -> Value :: term().
-optional_callbacks([
    init/2,
    encode/3,
    encode/4,
    decode/3,
    decode/4
]).

-type options() :: #{}.