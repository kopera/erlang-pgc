-module(pgsql_codec_network).
-behaviour(pgsql_codec).
-export([
    encodes/1,
    encode/4,
    decodes/1,
    decode/4
]).

-include("../../include/types.hrl").

encodes(_Opts) ->
    [macaddr_send, inet_send, cidr_send].

encode(#pgsql_type_info{send = macaddr_send}, #pgsql_macaddr{} = MAC, _Codec, _Opts) ->
    encode_mac(MAC);
encode(#pgsql_type_info{send = inet_send}, #pgsql_inet{} = INET, _Codec, _Opts) ->
    encode_inet(INET);
encode(#pgsql_type_info{send = inet_send}, #pgsql_cidr{} = CIDR, _Codec, _Opts) ->
    encode_cidr(CIDR);
encode(_Type, Value, _Codec, _Opts) ->
    error(badarg, [Value]).

decodes(_Opts) ->
    [macaddr_recv, inet_recv, cidr_recv].

decode(#pgsql_type_info{recv = macaddr_recv}, Data, _Codec, _Opts) ->
    decode_mac(Data);
decode(#pgsql_type_info{recv = inet_recv}, Data, _Codec, _Opts) ->
    decode_inet(Data);
decode(#pgsql_type_info{recv = cidr_recv}, Data, _Codec, _Opts) ->
    decode_cidr(Data).


% Internals

encode_mac(#pgsql_macaddr{address = {A, B, C, D, E, F}}) ->
    <<A, B, C, D, E, F>>.

encode_inet(#pgsql_inet{address = {A, B, C, D}}) ->
    <<2, 32, 0, 4, A, B, C, D>>;
encode_inet(#pgsql_inet{address = {A, B, C, D, E, F, G, H}}) ->
    <<3, 128, 0, 16, A:16, B:16, C:16, D:16, E:16, F:16, G:16, H:16>>.

encode_cidr(#pgsql_cidr{address = {A, B, C, D}, mask = Mask}) ->
    <<2, Mask, 1, 4, A, B, C, D>>;
encode_cidr(#pgsql_cidr{address = {A, B, C, D, E, F, G, H}, mask = Mask}) ->
    <<3, Mask, 1, 16, A:16, B:16, C:16, D:16, E:16, F:16, G:16, H:16>>.

decode_mac(<<A, B, C, D, E, F>>) ->
    #pgsql_macaddr{address = {A, B, C, D, E, F}}.

decode_inet(<<2, 32, 0, 4, A, B, C, D>>) ->
    #pgsql_inet{address = {A, B, C, D}};
decode_inet(<<3, 128, 0, 16, A:16, B:16, C:16, D:16, E:16, F:16, G:16, H:16>>) ->
    #pgsql_inet{address = {A, B, C, D, E, F, G, H}}.

decode_cidr(<<2, Mask, 1, 4, A, B, C, D>>) ->
    #pgsql_cidr{address = {A, B, C, D}, mask = Mask};
decode_cidr(<<3, Mask, 1, 16, A:16, B:16, C:16, D:16, E:16, F:16, G:16, H:16>>) ->
    #pgsql_cidr{address = {A, B, C, D, E, F, G, H}, mask = Mask}.
