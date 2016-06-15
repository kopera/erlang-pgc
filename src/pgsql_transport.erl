-module(pgsql_transport).
-export([
    open/5,
    dup/1,
    send/2,
    recv/2,
    recv_buffered/2,
    close/1,
    set_opts/2,
    get_tags/1
]).
-export_type([
    transport/0
]).

-record(tcp_transport, {
    socket :: port(),
    peer :: {inet:ip_address(), inet:port_number()},
    connect_timeout :: timeout(),
    buffer = <<>> :: binary()
}).

-record(ssl_transport, {
    socket :: ssl:sslsocket(),
    peer :: {inet:ip_address(), inet:port_number()},
    opts :: [ssl:ssl_option()],
    connect_timeout :: timeout(),
    buffer = <<>> :: binary()
}).

-opaque transport() :: #tcp_transport{} | #ssl_transport{}.
-type message() :: pgsql_protocol:message().

-include("./pgsql_protocol.hrl").


open(Host, Port, SSL, SSLOpts, Timeout) ->
    case gen_tcp:connect(Host, Port, [binary, {packet, raw}, {active, false}, {keepalive, true}], Timeout) of
        {ok, Socket} when SSL =:= disable ->
            {ok, Peer} = inet:peername(Socket),
            {ok, #tcp_transport{socket = Socket, peer = Peer, connect_timeout = Timeout}};
        {ok, Socket} ->
            ok = gen_tcp:send(Socket, encode_message(#msg_ssl_request{})),
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

dup(#tcp_transport{peer = {Host, Port}, connect_timeout = Timeout}) ->
    open(Host, Port, disable, [], Timeout);
dup(#ssl_transport{peer = {Host, Port}, opts = Opts, connect_timeout = Timeout}) ->
    open(Host, Port, require, Opts, Timeout).

-spec send(transport(), [message()]) -> ok | {error, any()}.
send(Transport, Messages) ->
    send_data(Transport, encode_messages(Messages)).

-spec recv(transport(), timeout()) -> {ok, message(), transport()} | {error, any()}.
recv(Transport, Timeout) ->
    case recv_buffered(Transport, <<>>) of
        {ok, _, _} = Result ->
            Result;
        {incomplete, Missing, Transport1} ->
            case recv_data(Transport1, Missing, Timeout) of
                {ok, Data} ->
                    recv_buffered(Transport1, Data);
                Error ->
                    Error
            end
    end.

-spec recv_buffered(transport(), binary()) -> {ok, message(), transport()} | {incomplete, pos_integer(), transport()}.
recv_buffered(#tcp_transport{buffer = Buffer} = Transport, Data) ->
    case decode_message(<<Buffer/binary, Data/binary>>) of
        {ok, Message, Rest} ->
            {ok, Message, Transport#tcp_transport{buffer = Rest}};
        {incomplete, _} ->
            {incomplete, Transport#tcp_transport{buffer = <<Buffer/binary, Data/binary>>}}
    end;
recv_buffered(#ssl_transport{buffer = Buffer} = Transport, Data) ->
    case decode_message(<<Buffer/binary, Data/binary>>) of
        {ok, Message, Rest} ->
            {ok, Message, Transport#ssl_transport{buffer = Rest}};
        {incomplete, Missing} ->
            {incomplete, Missing, Transport#ssl_transport{buffer = <<Buffer/binary, Data/binary>>}}
    end.

close(#tcp_transport{socket = Socket}) ->
    gen_tcp:close(Socket);
close(#ssl_transport{socket = Socket}) ->
    ssl:close(Socket).

set_opts(#tcp_transport{socket =  Socket}, Opts) ->
    inet:setopts(Socket, Opts);
set_opts(#ssl_transport{socket =  Socket}, Opts) ->
    ssl:setopts(Socket, Opts).

get_tags(#tcp_transport{socket = Socket}) ->
    {tcp, tcp_closed, tcp_error, Socket};
get_tags(#ssl_transport{socket = Socket}) ->
    {ssl, ssl_closed, ssl_error, Socket}.


%% Internals

send_data(#tcp_transport{socket = Socket}, Data) ->
    gen_tcp:send(Socket, Data);
send_data(#ssl_transport{socket = Socket}, Data) ->
    ssl:send(Socket, Data).

recv_data(#tcp_transport{socket = Socket}, Length, Timeout) ->
    gen_tcp:recv(Socket, Length, Timeout);
recv_data(#ssl_transport{socket = Socket}, Length, Timeout) ->
    ssl:recv(Socket, Length, Timeout).

decode_message(<<Code:8/integer, Size:32/integer, Available/binary>>) ->
    PayloadSize = Size - 4,
    AvailableSize = byte_size(Available),
    if
        PayloadSize =< AvailableSize ->
            <<Payload:PayloadSize/binary, Rest/binary>> = Available,
            {ok, pgsql_protocol:decode(Code, Payload), Rest};
        PayloadSize > AvailableSize ->
            {incomplete, PayloadSize - AvailableSize}
    end;
decode_message(Data) ->
    {incomplete, 5 - byte_size(Data)}.

encode_messages(Messages) ->
    [encode_message(Message) || Message <- Messages].

encode_message(Message) ->
    {Type, Payload} = pgsql_protocol:encode(Message),
    [Type, <<(iolist_size(Payload) + 4):32/integer>>, Payload].
