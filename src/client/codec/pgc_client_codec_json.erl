-module(pgc_client_codec_json).
-moduledoc false.

-behaviour(pgc_client_codec).
-export([
    names/0,
    encode/3,
    decode/3
]).

names() ->
    [~"json_send", ~"json_recv"].

-doc """
Plugs in a JSON library via this call's `codecs => #{json => #{codec => Module}}` option
(`Module:encode/1`/`Module:decode/1`) -- default is passthrough, i.e. the caller already deals
in raw JSON text.
""".
encode(Term, _TypeDescriptor, Codecs) ->
    case maps:get(codec, pgc_client_codecs:options(json, Codecs), undefined) of
        undefined -> _ = iolist_size(Term), Term;
        Module -> Module:encode(Term)
    end.

decode(Data, _TypeDescriptor, Codecs) ->
    case maps:get(codec, pgc_client_codecs:options(json, Codecs), undefined) of
        undefined -> Data;
        Module -> Module:decode(Data)
    end.
