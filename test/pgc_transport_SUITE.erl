-module(pgc_transport_SUITE).
-moduledoc false.

-behaviour(ct_suite).
-export([
    suite/0,
    init_per_suite/1,
    end_per_suite/1,
    all/0
]).

-export([
    tls_without_options_does_not_verify_test/1
]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("public_key/include/public_key.hrl").


-doc false.
suite() ->
    [{timetrap, {seconds, 30}}].

-doc false.
init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started([ssl, pgc]),
    {ok, Modules} = application:get_key(pgc, modules),
    lists:foreach(fun code:ensure_loaded/1, Modules),
    Config.

-doc false.
end_per_suite(_Config) ->
    ok = application:stop(pgc),
    ok.

-doc false.
all() ->
    [
        tls_without_options_does_not_verify_test
    ].


% ------------------------------------------------------------------------------
% Test cases
% ------------------------------------------------------------------------------

tls_without_options_does_not_verify_test(_Config) ->
    {Port, _CACerts} = start_server("other.pgc.test", 1),
    {ok, Transport} = pgc_transport:connect(#{host => "localhost", port => Port}, #{}, 5000),
    ?assertMatch({ok, _}, receive_server_name()),
    pgc_transport:close(Transport).


% ------------------------------------------------------------------------------
% Helpers
% ------------------------------------------------------------------------------

start_server(DNSName, Connections) ->
    #{server_config := ServerConfig, client_config := ClientConfig} = public_key:pkix_test_data(#{
        server_chain => #{
            root => [{key, {namedCurve, secp256r1}}, {digest, sha256}],
            intermediates => [],
            peer => [{key, {namedCurve, secp256r1}}, {digest, sha256}, {extensions, [#'Extension'{
                extnID = ?'id-ce-subjectAltName',
                extnValue = [{dNSName, DNSName}],
                critical = false
            }]}]
        },
        client_chain => #{
            root => [{key, {namedCurve, secp256r1}}, {digest, sha256}],
            intermediates => [],
            peer => [{key, {namedCurve, secp256r1}}, {digest, sha256}]
        }
    }),
    Parent = self(),
    Server = spawn_link(fun() ->
        {ok, Listen} = gen_tcp:listen(0, [binary, {active, false}, {ip, {127, 0, 0, 1}}]),
        {ok, Port} = inet:port(Listen),
        Parent ! {self(), port, Port},
        serve(Parent, Listen, ServerConfig, Connections)
    end),
    receive
        {Server, port, Port} -> {Port, proplists:get_value(cacerts, ClientConfig)}
    end.

serve(_Parent, _Listen, _ServerConfig, 0) ->
    receive stop -> ok end;
serve(Parent, Listen, ServerConfig, Connections) ->
    {ok, Socket} = gen_tcp:accept(Listen),
    {ok, <<8:32/integer, 1234:16/integer, 5679:16/integer>>} = gen_tcp:recv(Socket, 8),
    ok = gen_tcp:send(Socket, <<$S>>),
    case ssl:handshake(Socket, ServerConfig, 5000) of
        {ok, TLSSocket} ->
            {ok, Information} = ssl:connection_information(TLSSocket, [sni_hostname]),
            Parent ! {server_name, proplists:get_value(sni_hostname, Information)};
        {error, Reason} ->
            Parent ! {handshake_failed, Reason}
    end,
    serve(Parent, Listen, ServerConfig, Connections - 1).

receive_server_name() ->
    receive
        {server_name, ServerName} -> {ok, ServerName};
        {handshake_failed, Reason} -> {error, Reason}
    after 5000 ->
        ct:fail(no_handshake)
    end.
