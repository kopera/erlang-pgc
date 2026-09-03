-module(pgc_transport).
-export([
    connect/3,
    dup/2,
    send/2,
    % recv/2,
    close/1,
    % shutdown/2,
    set_active/2,
    % set_owner/2,
    handle_message/2
]).
-export_record([
    error
]).
-export_type([
    t/0,
    address/0,
    connect_options/0,
    error/0
]).


% ------------------------------------------------------------------------------
% Errors
% ------------------------------------------------------------------------------

-record #error{
    reason ::
        timeout
        | connection_refused    % econnrefused
        | connection_reset      % econnreset
        | host_unreachable      % ehostunreach
        | network_unreachable   % enetunreach
        | name_not_resolved     % nxdomain
        | permission_denied     % eacces
        | not_found             % enoent -- unix-socket path doesn't exist
        | tls_unavailable
        | {tls_failed, ssl:reason()}
        | system_limit          % system_limit | emfile | enfile
}.
-type error() :: #error{}.

% ------------------------------------------------------------------------------
% Types
% ------------------------------------------------------------------------------

-record #tcp_transport{
    socket :: gen_tcp:socket(),
    options :: connect_options()
}.
-record #tls_transport{
    socket :: ssl:sslsocket(),
    options :: connect_options()
}.
-opaque t() :: #tcp_transport{} | #tls_transport{}.

-type address() :: #{
    host := inet:hostname() | inet:ip_address(),
    port := socket:port_number()
} | #{
    path := binary() | string()
}.


-spec connect(address(), connect_options(), timeout()) -> {ok, t()} | {error, error()}.
-type connect_options() :: #{
    tls => disable | prefer | require,
    tls_options => [ssl:tls_client_option()]
}.
connect(Address, Options, Timeout) ->
    Deadline = pgc_deadline:from_timeout(Timeout),
    TLS = maps:get(tls, Options, prefer),
    TLSOptions = maps:get(tls_options, Options, []),
    case tcp_connect(Address, Deadline) of
        {ok, Socket} when TLS =:= disable ->
            {ok, #tcp_transport{socket = Socket, options = Options}};
        {ok, Socket} ->
            ok = gen_tcp:send(Socket, <<8:32/integer, 1234:16/integer, 5679:16/integer>>),
            case gen_tcp:recv(Socket, 1, pgc_deadline:to_timeout(Deadline)) of
                {ok, <<$S>>} ->
                    case tls_connect(Socket, TLSOptions, Deadline) of
                        {ok, TLSSocket} ->
                            {ok, #tls_transport{socket = TLSSocket, options = Options}};
                        % {error, _Error} when TLS =:= prefer ->
                        %     connect(Address, Options#{tls := disable}, pgc_deadline:to_timeout(Deadline));
                        % elp:ignore W0027
                        {error, Reason} ->
                            {error, Reason}
                    end;
                {ok, <<$N>>} when TLS =:= prefer ->
                    {ok, #tcp_transport{socket = Socket, options = Options}};
                {ok, <<$N>>} when TLS =:= require ->
                    _ = gen_tcp:close(Socket),
                    {error, #error{reason = tls_unavailable}};
                % elp:ignore W0027
                {error, timeout} ->
                    _ = gen_tcp:close(Socket),
                    {error, #error{reason = timeout}}
            end;
        % elp:ignore W0027
        {error, Error} ->
            {error, Error}
    end.

-spec tcp_connect(address(), pgc_deadline:t()) -> {ok, gen_tcp:socket()} | {error, #error{}}.
tcp_connect(Address, Deadline) ->
    Options = [
        {inet_backend, socket},
        binary,
        {packet, raw},
        {active, false}
    ],
    Host = case Address of
        #{host := H} -> H;
        #{path := Path} -> {local, Path}
    end,
    Port = case Address of
        #{port := P} -> P;
        #{path := _} -> 0
    end,
    Timeout = pgc_deadline:to_timeout(Deadline),
    case gen_tcp:connect(Host, Port, Options, Timeout) of
        {ok, Socket} ->
            {ok, Socket};
        % elp:ignore W0027
        {error, Reason}->
            {error, #error{reason = tcp_socket_error(Reason)}}
    end.


-spec tls_connect(gen_tcp:socket(), [ssl:tls_client_option()], pgc_deadline:t()) -> {ok, ssl:sslsocket()} | {error, #error{}}.
tls_connect(Socket, TLSOptions, Deadline) ->
    Timeout = pgc_deadline:to_timeout(Deadline),
    case ssl:connect(Socket, TLSOptions, Timeout) of
        {ok, TLSSocket} ->
            {ok, TLSSocket};
        % elp:ignore W0027
        {error, Reason} ->
            _ = gen_tcp:close(Socket),
            {error, #error{reason = tls_socket_error(Reason)}}
    end.


dup(#tcp_transport{socket = Socket, options = Options}, Timeout) ->
    case inet:peername(Socket) of
        {ok, {local, Path}} ->
            connect(#{path => Path}, Options, Timeout);
        {ok, {Address, Port}} when is_tuple(Address), (tuple_size(Address) =:= 4 orelse tuple_size(Address) =:= 8) ->
            connect(#{host => Address, port => Port}, Options, Timeout)
    end;
dup(#tls_transport{socket = Socket, options = Options}, Timeout) ->
    case ssl:peername(Socket) of
        {ok, {local, Path}} ->
            connect(#{path => Path}, Options, Timeout);
        {ok, {Address, Port}} when is_tuple(Address), (tuple_size(Address) =:= 4 orelse tuple_size(Address) =:= 8) ->
            connect(#{host => Address, port => Port}, Options, Timeout)
    end.


-spec send(t(), iodata()) -> ok | {error, error()}.
send(#tcp_transport{socket = Socket}, Data) ->
    case gen_tcp:send(Socket, Data) of
        ok -> ok;
        % elp:ignore W0027
        {error, Reason} -> {error, #error{reason = tcp_socket_error(Reason)}}
    end;
send(#tls_transport{socket = Socket}, Data) ->
    case ssl:send(Socket, Data) of
        ok -> ok;
        % elp:ignore W0027
        {error, Reason} -> {error, #error{reason = tls_socket_error(Reason)}}
    end.

-spec close(t()) -> ok.
close(#tcp_transport{socket = Socket}) ->
    ok = gen_tcp:close(Socket);
close(#tls_transport{socket = Socket}) ->
    _ = ssl:close(Socket),
    ok.

-spec set_active(t(), boolean() | once | -32768..32767) -> ok | {error, any()}.
set_active(#tcp_transport{socket =  Socket}, Active) ->
    inet:setopts(Socket, [{active, Active}]);
set_active(#tls_transport{socket =  Socket}, Active) ->
    ssl:setopts(Socket, [{active, Active}]).

-doc """
Classifies an inbound process message against this transport, so callers
never need to know the underlying socket module's message tags or match on
the raw socket handle.

Returns `unknown` for any message that isn't one of this transport's own
notifications (e.g. an unrelated `info` message the caller's process
received) so callers can safely feed every unmatched `info` message through
this function.
""".
-spec handle_message(t(), term()) -> {data, binary()} | {error, error()} | unknown.
handle_message(#tcp_transport{socket = Socket}, {tcp, Socket, Data}) when is_binary(Data) ->
    {data, Data};
handle_message(#tcp_transport{socket = Socket}, {tcp_closed, Socket}) ->
    {error, #error{reason = connection_reset}};
handle_message(#tcp_transport{socket = Socket}, {tcp_error, Socket, Reason}) ->
    {error, #error{reason = tcp_socket_error(Reason)}};

handle_message(#tls_transport{socket = Socket}, {ssl, Socket, Data}) when is_binary(Data) ->
    {data, Data};
handle_message(#tls_transport{socket = Socket}, {ssl_closed, Socket}) ->
    {error, #error{reason = connection_reset}};
handle_message(#tls_transport{socket = Socket}, {ssl_error, Socket, Reason}) ->
    {error, #error{reason = tls_socket_error(Reason)}};

handle_message(_Transport, _Message) ->
    unknown.


tcp_socket_error(Reason) when Reason =:= timeout; Reason =:= etimedout ->
    timeout;
tcp_socket_error(econnrefused) ->
    connection_refused;
tcp_socket_error(Reason) when Reason =:= closed; Reason =:= econnreset ->
    connection_reset;
tcp_socket_error(ehostunreach) ->
    host_unreachable;
tcp_socket_error(enetunreach) ->
    network_unreachable;
tcp_socket_error(nxdomain) ->
    name_not_resolved;
tcp_socket_error(eacces) ->
    permission_denied;
tcp_socket_error(enoent) ->
    not_found;
tcp_socket_error(Reason) when Reason =:= system_limit; Reason =:= emfile; Reason =:= enfile ->
    system_limit.


tls_socket_error(Reason) when Reason =:= timeout; Reason =:= etimedout ->
    timeout;
tls_socket_error(Reason) when Reason =:= closed; Reason =:= econnreset ->
    connection_reset;
tls_socket_error(Reason) ->
    {tls_failed, Reason}.
