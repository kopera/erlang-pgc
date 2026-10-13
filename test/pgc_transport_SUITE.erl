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
    sends_server_name_test/1,
    sends_server_name_without_trailing_dot_test/1,
    atom_host_sends_server_name_test/1,
    ip_address_sends_no_server_name_test/1,
    keeps_given_server_name_test/1,
    dup_sends_server_name_test/1,
    tls_without_options_does_not_verify_test/1,
    verify_full_checks_hostname_test/1,
    verify_full_accepts_wildcard_test/1
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
        sends_server_name_test,
        sends_server_name_without_trailing_dot_test,
        atom_host_sends_server_name_test,
        ip_address_sends_no_server_name_test,
        keeps_given_server_name_test,
        dup_sends_server_name_test,
        tls_without_options_does_not_verify_test,
        verify_full_checks_hostname_test,
        verify_full_accepts_wildcard_test
    ].


% ------------------------------------------------------------------------------
% Test cases
% ------------------------------------------------------------------------------

sends_server_name_test(_Config) ->
    {Port, _CACerts} = start_server("localhost", 1),
    {ok, Transport} = pgc_transport:connect(#{host => "localhost", port => Port}, #{tls => require}, 5000),
    ?assertEqual({ok, "localhost"}, receive_server_name()),
    pgc_transport:close(Transport).

sends_server_name_without_trailing_dot_test(_Config) ->
    {Port, _CACerts} = start_server("localhost", 1),
    {ok, Transport} = pgc_transport:connect(#{host => "localhost.", port => Port}, #{tls => require}, 5000),
    ?assertEqual({ok, "localhost"}, receive_server_name()),
    pgc_transport:close(Transport).

atom_host_sends_server_name_test(_Config) ->
    {Port, _CACerts} = start_server("localhost", 1),
    {ok, Transport} = pgc_transport:connect(#{host => localhost, port => Port}, #{tls => require}, 5000),
    ?assertEqual({ok, "localhost"}, receive_server_name()),
    pgc_transport:close(Transport).

ip_address_sends_no_server_name_test(_Config) ->
    {Port, _CACerts} = start_server("localhost", 1),
    {ok, Transport} = pgc_transport:connect(#{host => "127.0.0.1", port => Port}, #{tls => require}, 5000),
    ?assertEqual({ok, undefined}, receive_server_name()),
    pgc_transport:close(Transport).

keeps_given_server_name_test(_Config) ->
    {Port, _CACerts} = start_server("localhost", 1),
    {ok, Transport} = pgc_transport:connect(#{host => "localhost", port => Port}, #{
        tls => require,
        tls_options => [{verify, verify_none}, {server_name_indication, "db.pgc.test"}]
    }, 5000),
    ?assertEqual({ok, "db.pgc.test"}, receive_server_name()),
    pgc_transport:close(Transport).

dup_sends_server_name_test(_Config) ->
    {Port, _CACerts} = start_server("localhost", 2),
    {ok, Transport} = pgc_transport:connect(#{host => "localhost", port => Port}, #{tls => require}, 5000),
    ?assertEqual({ok, "localhost"}, receive_server_name()),
    {ok, Dup} = pgc_transport:dup(Transport, 5000),
    ?assertEqual({ok, "localhost"}, receive_server_name()),
    pgc_transport:close(Dup),
    pgc_transport:close(Transport).

tls_without_options_does_not_verify_test(_Config) ->
    {Port, _CACerts} = start_server("other.pgc.test", 1),
    {ok, Transport} = pgc_transport:connect(#{host => "localhost", port => Port}, #{}, 5000),
    ?assertMatch({ok, _}, receive_server_name()),
    pgc_transport:close(Transport).

verify_full_checks_hostname_test(_Config) ->
    {Port, CACerts} = start_server("other.pgc.test", 1),
    ?assertMatch(
        {error, #pgc_transport:error{reason = {tls_failed, {tls_alert, {bad_certificate, _}}}}},
        pgc_transport:connect(#{host => "localhost", port => Port}, sslmode_options("verify-full", CACerts), 5000)
    ),
    {Port2, CACerts2} = start_server("localhost", 1),
    {ok, Transport} = pgc_transport:connect(#{host => "localhost", port => Port2}, sslmode_options("verify-full", CACerts2), 5000),
    pgc_transport:close(Transport).

verify_full_accepts_wildcard_test(_Config) ->
    {Port, CACerts} = start_server("*.pgc.test", 1),
    #{tls_options := TLSOptions} = Options = sslmode_options("verify-full", CACerts),
    {ok, Transport} = pgc_transport:connect(#{host => "localhost", port => Port}, Options#{
        tls_options := [{server_name_indication, "db.pgc.test"} | TLSOptions]
    }, 5000),
    ?assertEqual({ok, "db.pgc.test"}, receive_server_name()),
    pgc_transport:close(Transport).


% ------------------------------------------------------------------------------
% Helpers
% ------------------------------------------------------------------------------

sslmode_options(SSLMode, CACerts) ->
    {ok, #{tls := TLS, tls_options := TLSOptions}} =
        pgc_client_options:from_uri("postgresql://user@localhost/db?sslmode=" ++ SSLMode),
    #{tls => TLS, tls_options => lists:keystore(cacerts, 1, TLSOptions, {cacerts, CACerts})}.

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
