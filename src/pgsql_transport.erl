-module(pgsql_transport).
-export([
    open/5,
    dup/1,
    send/2,
    recv/2,
    close/1,
    set_opts/2,
    get_tags/1
]).
-export_type([
    transport/0,
    tags/0
]).

-record(tcp_transport, {
    socket :: port(),
    peer :: {inet:ip_address(), inet:port_number()},
    connect_timeout :: timeout()
}).

-record(ssl_transport, {
    socket :: ssl:sslsocket(),
    peer :: {inet:ip_address(), inet:port_number()},
    opts :: [ssl:ssl_option()],
    connect_timeout :: timeout()
}).

-opaque transport() :: #tcp_transport{} | #ssl_transport{}.
-include("./pgsql_protocol_messages.hrl").


-spec open(Host, Port, SSL, SSLOpts, Timeout) -> {ok, Transport} | {error, Error} when
    Host :: inet:ip_address() | inet:hostname(),
    Port :: inet:port_number(),
    SSL :: disable | prefer | require,
    SSLOpts :: [ssl:ssl_option()],
    Timeout :: timeout(),
    Transport :: transport(),
    Error :: ssl_not_available | inet:posix() | any().
open(Host, Port, SSL, SSLOpts, Timeout) ->
    case gen_tcp:connect(Host, Port, [binary, {packet, raw}, {active, false}, {keepalive, true}], Timeout) of
        {ok, Socket} when SSL =:= disable ->
            {ok, Peer} = inet:peername(Socket),
            {ok, #tcp_transport{socket = Socket, peer = Peer, connect_timeout = Timeout}};
        {ok, Socket} ->
            ok = gen_tcp:send(Socket, pgsql_protocol:encode_messages([#msg_ssl_request{}])),
            case gen_tcp:recv(Socket, 1, Timeout) of
                {ok, <<$S>>} ->
                    case ssl:connect(Socket, SSLOpts, Timeout) of
                        {ok, SSLSocket} ->
                            {ok, Peer} = ssl:peername(SSLSocket),
                            {ok, #ssl_transport{socket = SSLSocket, peer = Peer, opts = SSLOpts, connect_timeout = Timeout}};
                        {error, _Error} when SSL =:= prefer ->
                            _ = gen_tcp:close(Socket),
                            open(Host, Port, disable, SSLOpts, Timeout);
                        {error, Error} when SSL =:= require ->
                            _ = gen_tcp:close(Socket),
                            {error, {ssl_error, Error}}
                    end;
                {ok, <<$N>>} when SSL =:= prefer ->
                    {ok, Peer} = inet:peername(Socket),
                    {ok, #tcp_transport{socket = Socket, peer = Peer, connect_timeout = Timeout}};
                {ok, <<$N>>} when SSL =:= require ->
                    _ = gen_tcp:close(Socket),
                    {error, ssl_not_available};
                {error, timeout} = Error ->
                    _ = gen_tcp:close(Socket),
                    Error
            end;
        {error, Error} ->
            {error, Error}
    end.

-spec dup(transport()) -> {ok, transport()} | {error, Error} when
    Error :: ssl_not_available | inet:posix() | any().
dup(#tcp_transport{peer = {Host, Port}, connect_timeout = Timeout}) ->
    open(Host, Port, disable, [], Timeout);
dup(#ssl_transport{peer = {Host, Port}, opts = Opts, connect_timeout = Timeout}) ->
    open(Host, Port, require, Opts, Timeout).

-spec send(transport(), iodata()) -> ok | {error, any()}.
send(#tcp_transport{socket = Socket}, Data) ->
    gen_tcp:send(Socket, Data);
send(#ssl_transport{socket = Socket}, Data) ->
    ssl:send(Socket, Data).

-spec recv(transport(), non_neg_integer()) -> {ok, binary()} | {error, any()}.
recv(#tcp_transport{socket = Socket}, Length) ->
    gen_tcp:recv(Socket, Length);
recv(#ssl_transport{socket = Socket}, Length) ->
    ssl:recv(Socket, Length).

-spec close(transport()) -> ok.
close(#tcp_transport{socket = Socket}) ->
    gen_tcp:close(Socket);
close(#ssl_transport{socket = Socket}) ->
    ssl:close(Socket),
    ok.

-spec set_opts(transport(), [inet:socket_setopt()]) -> ok | {error, any()}.
set_opts(#tcp_transport{socket =  Socket}, Opts) ->
    inet:setopts(Socket, Opts);
set_opts(#ssl_transport{socket =  Socket}, Opts) ->
    ssl:setopts(Socket, Opts).

-spec get_tags(transport()) -> tags().
-type tags() :: {tcp, tcp_closed, tcp_error, port()} | {ssl, ssl_closed, ssl_error, ssl:sslsocket()}.
get_tags(#tcp_transport{socket = Socket}) ->
    {tcp, tcp_closed, tcp_error, Socket};
get_tags(#ssl_transport{socket = Socket}) ->
    {ssl, ssl_closed, ssl_error, Socket}.
