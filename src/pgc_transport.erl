%% @private
-module(pgc_transport).
-export([
    connect/1,
    dup/1,
    send/2,
    recv/2,
    close/1,
    shutdown/2,
    set_active/2,
    set_owner/2,
    get_tags/1
]).
-export_type([
    t/0,
    tags/0,
    send_error/0
]).

-record(tcp_transport, {
    socket :: gen_tcp:socket(),
    options :: pgc:transport_options()
}).

-record(tls_transport, {
    socket :: ssl:sslsocket(),
    options :: pgc:transport_options()
}).

-opaque t() :: #tcp_transport{} | #tls_transport{}.

-spec connect(pgc:transport_options()) -> {ok, t()} | {error, connect_error()}.
-type connect_error() :: econnrefused | timeout | {tls, unavailable | any()}.
connect(#{address := {tcp, Host, Port}} = Options) ->
    TLS = maps:get(tls, Options, prefer),
    TLSOptions = maps:get(tls_options, Options, []),
    Timeout = maps:get(connect_timeout, Options, 15_000),
    Deadline = pgc_deadline:from_timeout(Timeout),
    ConnectOptions = [
        binary,
        {packet, raw},
        {active, false},
        {keepalive, true}
    ],
    case gen_tcp:connect(Host, Port, ConnectOptions, pgc_deadline:to_timeout(Deadline)) of
        {ok, Socket} when TLS =:= disable ->
            {ok, #tcp_transport{
                socket = Socket,
                options = Options#{
                    address := peer_address(Socket)
                }
            }};
        {ok, Socket} ->
            ok = gen_tcp:send(Socket, <<8:32/integer, 1234:16/integer, 5679:16/integer>>),
            case gen_tcp:recv(Socket, 1, pgc_deadline:to_timeout(Deadline)) of
                {ok, <<$S>>} ->
                    case ssl:connect(Socket, TLSOptions, pgc_deadline:to_timeout(Deadline)) of
                        {ok, TLSSocket} ->
                            {ok, #tls_transport{
                                socket = TLSSocket,
                                options = Options#{
                                    address := peer_address(Socket)
                                }
                            }};
                        {error, timeout} = Error ->
                            _ = gen_tcp:close(Socket),
                            Error;
                        {error, _Error} when TLS =:= prefer ->
                            _ = gen_tcp:close(Socket),
                            connect(maps:put(tls, disable, Options));
                        {error, Error} when TLS =:= require ->
                            _ = gen_tcp:close(Socket),
                            {error, {tls, Error}}
                    end;
                {ok, <<$N>>} when TLS =:= prefer ->
                    {ok, #tcp_transport{
                        socket = Socket,
                        options = Options#{
                            address := peer_address(Socket)
                        }
                    }};
                {ok, <<$N>>} when TLS =:= require ->
                    _ = gen_tcp:close(Socket),
                    {error, {tls, unavailable}};
                {error, timeout} = Error ->
                    _ = gen_tcp:close(Socket),
                    Error
            end;
        {error, econnrefused} ->
            {error, econnrefused};
        {error, timeout} ->
            {error, timeout}
    end.

-spec dup(t()) -> {ok, t()} | {error, connect_error()}.
dup(#tcp_transport{options = ConnectOptions}) ->
    connect(ConnectOptions);
dup(#tls_transport{options = ConnectOptions}) ->
    connect(ConnectOptions).


-spec send(t(), iodata()) -> ok | {error, send_error()}.
-type send_error() :: term().
send(#tcp_transport{socket = Socket}, Data) ->
    gen_tcp:send(Socket, Data);
send(#tls_transport{socket = Socket}, Data) ->
    ssl:send(Socket, Data).


-spec recv(t(), non_neg_integer()) -> {ok, binary()} | {error, any()}.
recv(#tcp_transport{socket = Socket}, Length) ->
    case gen_tcp:recv(Socket, Length) of
        {ok, Binary} when is_binary(Binary) -> {ok, Binary};
        {error, _} = Error -> Error
    end;
recv(#tls_transport{socket = Socket}, Length) ->
    case ssl:recv(Socket, Length) of
        {ok, Binary} when is_binary(Binary) -> {ok, Binary};
        {error, _} = Error -> Error
    end.


-spec close(t()) -> ok.
close(#tcp_transport{socket = Socket}) ->
    ok = gen_tcp:close(Socket);
close(#tls_transport{socket = Socket}) ->
    _ = ssl:close(Socket),
    ok.


-spec shutdown(t(), read | write | read_write) -> ok.
shutdown(#tcp_transport{socket = Socket}, How) ->
    ok = gen_tcp:shutdown(Socket, How);
shutdown(#tls_transport{socket = Socket}, How) ->
    _ = ssl:shutdown(Socket, How),
    ok.

-spec set_active(t(), boolean() | once | -32768..32767) -> ok | {error, any()}.
set_active(#tcp_transport{socket =  Socket}, Active) ->
    inet:setopts(Socket, [{active, Active}]);
set_active(#tls_transport{socket =  Socket}, Active) ->
    ssl:setopts(Socket, [{active, Active}]).


-spec set_owner(t(), pid()) -> ok | {error, closed}.
set_owner(#tcp_transport{socket =  Socket}, NewOwner) ->
    case gen_tcp:controlling_process(Socket, NewOwner) of
        ok -> ok;
        {error, closed} -> {error, closed};
        {error, not_owner} -> erlang:error(not_owner)
    end;
set_owner(#tls_transport{socket =  Socket}, NewOwner) ->
    case ssl:controlling_process(Socket, NewOwner) of
        ok -> ok;
        {error, closed} -> {error, closed};
        {error, not_owner} -> erlang:error(not_owner)
    end.


-spec get_tags(t()) -> tags().
-type tags() :: {gen_tcp:socket(), tcp, tcp_closed, tcp_error, tcp_passive} | {ssl:sslsocket(), ssl, ssl_closed, ssl_error, ssl_passive}.
get_tags(#tcp_transport{socket = Socket}) ->
    {Socket, tcp, tcp_closed, tcp_error, tcp_passive};
get_tags(#tls_transport{socket = Socket}) ->
    {Socket, ssl, ssl_closed, ssl_error, ssl_passive}.


%% @private
-spec peer_address(gen_tcp:socket()) -> pgc:transport_address().
peer_address(Socket) ->
    case inet:peername(Socket) of
        {ok, {Address, Port}}
                when (tuple_size(Address) =:= 4 orelse tuple_size(Address) =:= 8)
                    andalso is_integer(Port)
                    andalso Port > 0
                    andalso Port =< 65535 ->
            {tcp, Address, Port}
    end.