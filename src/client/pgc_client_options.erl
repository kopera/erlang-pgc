-module(pgc_client_options).
-moduledoc """
Builds `t:t/0` maps, suitable for `pgc_client:start_link/1,2` (and, through it,
`pgc:start_link/2,3`).
""".

-export([
    from_uri/1
]).
-export_type([
    t/0
]).

-type t() :: #{
    address := pgc_transport:address(),
    tls => disable | prefer | require,
    tls_options => [ssl:tls_client_option()],
    connect_timeout => timeout(),
    ping_interval => timeout(),

    user := unicode:chardata(),
    password => unicode:chardata() | fun(() -> unicode:chardata()),
    database := unicode:chardata(),
    parameters => #{
        atom() => unicode:chardata()
    }
}.

-define(DEFAULT_PORT, 5432).
-define(DEFAULT_PING_INTERVAL, 30_000).

% -----------------------------------------------------------------------------
% Types
% -----------------------------------------------------------------------------

-type query() :: #{unicode:chardata() => unicode:chardata() | true}.


% -----------------------------------------------------------------------------
% API
% -----------------------------------------------------------------------------

-doc """
Parses a PostgreSQL connection URI, as documented at
https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING.

Supports a single `host[:port]` authority (no comma-separated multi-host failover lists),
`user[:password]@`, `/database`, and the `host`, `port`, `sslmode`, `connect_timeout` query
parameters plus a fixed set of session parameters (`application_name`, `client_encoding`,
`datestyle`, `timezone`, `search_path`, `options`, `replication`) -- anything else in the query
string is rejected rather than silently dropped, since a typo'd or unsupported parameter
(`sslrootcert`, `target_session_attrs`, ...) failing loudly beats it being quietly ignored.
""".
-spec from_uri(unicode:chardata()) -> {ok, t()} | {error, from_uri_error()}.
-type from_uri_error() ::
    {invalid_uri, {atom(), term()}}
    | {invalid_scheme, unicode:chardata()}
    | {invalid_port, unicode:chardata()}
    | {invalid_userinfo, unicode:chardata()}
    | {invalid_parameter, unicode:chardata(), unicode:chardata() | true}
    | {unsupported_parameter, unicode:chardata()}.
from_uri(Uri) ->
    case uri_string:normalize(pgc_string:characters_to_binary(Uri), [return_map]) of
        {error, Type, Term} ->
            {error, {invalid_uri, {Type, Term}}};
        #{scheme := Scheme} = UriMap when Scheme =:= ~"postgresql"; Scheme =:= ~"postgres" ->
            case uri_string:dissect_query(maps:get(query, UriMap, ~"")) of
                {error, Type, Term} ->
                    {error, {invalid_uri, {Type, Term}}};
                Q ->
                    Query = maps:from_list(Q),
                    with_address(UriMap, Query, #{})
            end;
        #{scheme := Scheme} ->
            {error, {invalid_scheme, Scheme}}
    end.


-spec with_address(uri_string:uri_map(), query(), #{}) -> {ok, t()} | {error, from_uri_error()}.
with_address(UriMap, Query, Acc) ->
    case address(UriMap, Query) of
        {ok, Address} ->
            with_userinfo(UriMap, Query, Acc#{address => Address});
        {error, Reason} ->
            {error, Reason}
    end.


with_userinfo(UriMap, Query, #{address := _Address} = Acc) ->
    case userinfo(UriMap, Query) of
        {ok, UserInfo} ->
            with_database(UriMap, Query, maps:merge(Acc, UserInfo));
        {error, Reason} ->
            {error, Reason}
    end.


with_database(UriMap, Query, #{user := User} = Acc) ->
    case database(UriMap, Query) of
        {ok, Database} ->
            with_parameters(UriMap, Query, Acc#{
                database => case Database of
                    undefined -> User;
                    _ -> Database
                end
            });
        {error, Reason} ->
            {error, Reason}
    end.


-spec with_parameters(uri_string:uri_map(), query(), t()) -> {ok, t()} | {error, from_uri_error()}.
with_parameters(_UriMap, Query, Options) when is_map(Query) ->
    maps:fold(fun
        (_Key, _Value, {error, _} = Error) ->
            Error;
        (~"host", _Value, {ok, Acc}) ->
            {ok, Acc};
        (~"port", _Value, {ok, Acc}) ->
            {ok, Acc};
        (~"sslmode", Value, {ok, Acc}) ->
            case parse_sslmode(Value) of
                {ok, SslMode} -> {ok, maps:merge(Acc, SslMode)};
                error -> {error, {invalid_parameter, ~"sslmode", Value}}
            end;
        (~"connect_timeout", Value, {ok, Acc}) ->
            case parse_timeout(Value) of
                {ok, Timeout} -> {ok, Acc#{connect_timeout => Timeout}};
                error -> {error, {invalid_parameter, ~"connect_timeout", Value}}
            end;

        (~"application_name", Value, {ok, Acc}) ->
            with_string_parameter(application_name, Value, Acc);
        (~"client_encoding", Value, {ok, Acc}) ->
            with_string_parameter(client_encoding, Value, Acc);
        (~"datestyle", Value, {ok, Acc}) ->
            with_string_parameter(datestyle, Value, Acc);
        (~"timezone", Value, {ok, Acc}) ->
            with_string_parameter(timezone, Value, Acc);
        (~"search_path", Value, {ok, Acc}) ->
            with_string_parameter(search_path, Value, Acc);
        (~"options", Value, {ok, Acc}) ->
            with_string_parameter(options, Value, Acc);
        (~"keepalives", Value, {ok, Acc}) ->
            case Value of
                ~"1" ->
                    case Acc of
                        #{ping_interval := Interval} when Interval =/= infinity ->
                            {ok, Acc};
                        #{} ->
                            {ok, Acc#{ping_interval => ?DEFAULT_PING_INTERVAL}}
                    end;
                ~"0" ->
                    {ok, Acc#{ping_interval => infinity}};
                Other ->
                    {error, {invalid_parameter, ~"keepalives_idle", Other}}
            end;
        (~"keepalives_idle", Value, {ok, Acc}) ->
            case parse_timeout(Value) of
                {ok, 0} ->
                    {ok, Acc#{ping_interval => ?DEFAULT_PING_INTERVAL}};
                {ok, Timeout} ->
                    {ok, Acc#{ping_interval => Timeout}};
                error ->
                    {error, {invalid_parameter, ~"keepalives_idle", Value}}
            end;
        (Key, _Value, {ok, _Acc}) ->
            {error, {unsupported_parameter, Key}}
    end, {ok, Options}, Query).


-spec with_string_parameter(atom(), unicode:chardata() | true, t()) -> {ok, t()} | {error, from_uri_error()}.
with_string_parameter(Name, true = Value, _Options) ->
    {error, {invalid_parameter, atom_to_binary(Name), Value}};
with_string_parameter(Name, Value, Options) ->
    Parameters = maps:get(parameters, Options, #{}),
    {ok, Options#{
        parameters => Parameters#{
            Name => Value
        }
    }}.


-doc """
An empty or `/`-prefixed host (after percent-decoding) is a unix socket directory, per libpq's
own `postgresql://%2Fvar%2Frun%2Fpostgresql/mydb` / `postgresql:///mydb?host=/var/run/postgresql`
conventions -- the latter's `host` query parameter is only consulted when the URI has no
authority host of its own to begin with.
""".
-spec address(UriMap, Query) -> {ok, pgc_transport:address()} | {error, from_uri_error()} when
    UriMap :: uri_string:uri_map(),
    Query :: query().
address(UriMap, Query) ->
    case host(UriMap, Query) of
        {ok, <<"/", _/binary>> = Path} ->
            {ok, #{path => pgc_string:characters_to_binary(uri_string:unquote(Path))}};
        {ok, Host} ->
            case port(UriMap, Query) of
                {ok, Port} when Host =/= ~"" ->
                    {ok, #{host => pgc_string:characters_to_list(Host), port => Port}};
                {ok, Port}  ->
                    % TODO: default to unix socket path on unix and localhost on windows
                    {ok, #{host => "localhost", port => Port}};
                {error, _} = Error ->
                    Error
            end
    end.


-spec host(UriMap, Query) -> {ok, unicode:chardata()} | {error, from_uri_error()} when
    UriMap :: uri_string:uri_map(),
    Query :: query().
host(UriMap, Query) ->
    case maps:get(host, UriMap, ~"") of
        ~"" ->
            case maps:get(~"host", Query, ~"") of
                Host when is_binary(Host) -> {ok, Host};
                true -> {ok, ~""}
            end;
        Host ->
            {ok, uri_string:unquote(Host)}
    end.


-spec port(UriMap, Query) -> {ok, inet:port_number()} | {error, from_uri_error()} when
    UriMap :: uri_string:uri_map(),
    Query :: query().
port(UriMap, Query) ->
    case UriMap of
        #{port := Port} when is_integer(Port) ->
            {ok, Port};
        #{} ->
            case maps:find(~"port", Query) of
                {ok, PortString} when is_binary(PortString) ->
                    case string:to_integer(PortString) of
                        {Port, ~""} when Port >= 0; Port =< 16#ffff ->
                            {ok, Port};
                        _ ->
                            {error, {invalid_port, PortString}}
                    end;
                {ok, true} ->
                    {error, {invalid_port, ~""}};
                error ->
                    {ok, ?DEFAULT_PORT}
            end
    end.


-spec userinfo(UriMap, Query) -> {ok, UserInfo} | {error, from_uri_error()} when
    UriMap :: uri_string:uri_map(),
    Query :: query(),
    UserInfo :: #{user := unicode:chardata(), password => unicode:chardata()}.
userinfo(#{userinfo := UserInfo}, _Query) ->
    case binary:split(pgc_string:characters_to_binary(UserInfo), ~":") of
        [User] ->
            {ok, #{user => uri_string:unquote(User)}};
        [User, Password] ->
            {ok, #{user => uri_string:unquote(User), password => uri_string:unquote(Password)}};
        _ ->
            {error, {invalid_userinfo, UserInfo}}
    end;
userinfo(#{}, _Query) ->
    {error, {invalid_userinfo, ~""}}.


-spec database(UriMap, Query) -> {ok, Database | undefined} when
    UriMap :: uri_string:uri_map(),
    Query :: query(),
    Database :: unicode:chardata().
database(UriMap, _Query) ->
    case UriMap of
        #{path := ~""} -> {ok, undefined};
        #{path := ~"/"} -> {ok, undefined};
        #{path := <<"/", Rest/binary>>} -> {ok, Rest};
        #{path := Path} -> {ok, Path}
    end.


-spec parse_sslmode(Value) -> {ok, TlsOptions} | error when
    Value :: unicode:chardata() | true,
    TlsOptions :: #{tls => disable | prefer | require, tls_options => [ssl:tls_client_option()]}.
parse_sslmode(true) ->
    error;
parse_sslmode(Value) ->
    case pgc_string:characters_to_binary(Value) of
        ~"disable" ->
            {ok, #{tls => disable}};
        ~"allow" ->
            {ok, #{tls => prefer, tls_options => [{verify, verify_none}]}};
        ~"prefer" ->
            {ok, #{tls => prefer, tls_options => [{verify, verify_none}]}};
        ~"require" ->
            {ok, #{tls => require, tls_options => [{verify, verify_none}]}};
        ~"verify-ca" ->
            {ok, #{
                tls => require,
                tls_options => [
                    {verify, verify_peer},
                    {cacerts, public_key:cacerts_get()}
                ]
            }};
        ~"verify-full" ->
            {ok, #{
                tls => require,
                tls_options => [
                    {verify, verify_peer},
                    {cacerts, public_key:cacerts_get()}
                ]
            }};
        _Other ->
            error
    end.


-spec parse_timeout(Value) -> {ok, Timeout} | error when
    Value :: unicode:chardata() | true,
    Timeout :: timeout().
parse_timeout(true) ->
    error;
parse_timeout(Value) ->
    case string:to_integer(Value) of
        {Timeout, ~""} when Timeout >= 0 ->
            {ok, erlang:convert_time_unit(Timeout, second, millisecond)};
        _ ->
            error
    end.


-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

full_uri_test() ->
    ?assertMatch({ok, #{
        address := #{host := "localhost", port := 5433},
        user := ~"user",
        password := ~"p@ss",
        database := ~"mydb",
        tls := require,
        tls_options := [{verify, verify_none}],
        connect_timeout := 10000,
        parameters := #{application_name := ~"my app"}
    }}, pgc_client_options:from_uri(~"postgresql://user:p%40ss@localhost:5433/mydb?sslmode=require&connect_timeout=10&application_name=my%20app")).

default_port_and_database_test() ->
    ?assertEqual({ok, #{
        address => #{host => "localhost", port => ?DEFAULT_PORT},
        user => ~"postgres",
        database => ~"postgres"
    }}, pgc_client_options:from_uri(~"postgresql://postgres@localhost")).

postgres_scheme_test() ->
    ?assertMatch({ok, #{}}, pgc_client_options:from_uri(~"postgres://postgres@localhost/mydb")).

unix_socket_host_test() ->
    ?assertEqual({ok, #{
        address => #{path => ~"/var/run/postgresql"},
        user => ~"postgres",
        database => ~"mydb"
    }}, pgc_client_options:from_uri(~"postgresql://postgres@%2Fvar%2Frun%2Fpostgresql/mydb")).

unix_socket_host_query_param_test() ->
    ?assertEqual({ok, #{
        address => #{path => ~"/var/run/postgresql"},
        user => ~"postgres",
        database => ~"mydb"
    }}, pgc_client_options:from_uri(~"postgresql://postgres@/mydb?host=/var/run/postgresql")).

ipv6_host_test() ->
    ?assertMatch({ok, #{address := #{host := "::1", port := 5432}}},
        pgc_client_options:from_uri(~"postgresql://postgres@[::1]/mydb")).

missing_user_test() ->
    ?assertEqual({error, {invalid_userinfo, ~""}}, pgc_client_options:from_uri(~"postgresql://localhost/mydb")).

invalid_scheme_test() ->
    ?assertEqual({error, {invalid_scheme, ~"mysql"}}, pgc_client_options:from_uri(~"mysql://user@localhost/mydb")).

invalid_sslmode_test() ->
    ?assertEqual({error, {invalid_parameter, ~"sslmode" , ~"verify-nope"}},
        pgc_client_options:from_uri(~"postgresql://user@localhost/mydb?sslmode=verify-nope")).

unsupported_parameter_test() ->
    ?assertEqual({error, {unsupported_parameter, ~"sslrootcert"}},
        pgc_client_options:from_uri(~"postgresql://user@localhost/mydb?sslrootcert=/root.crt")).

invalid_uri_test() ->
    ?assertMatch({error, {invalid_uri, _}}, pgc_client_options:from_uri(~"not a uri")).

-endif.
