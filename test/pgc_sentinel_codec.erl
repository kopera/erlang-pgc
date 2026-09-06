-module(pgc_sentinel_codec).
-moduledoc "A codec module used by pgc_client_SUITE to prove that a per-call `codecs => #{modules => [...]}` override wins over `pgc_client_codec_builtin`.".

-export([
    int4recv/3
]).

int4recv(_Data, _TypeDescriptor, _Codecs) ->
    sentinel.
