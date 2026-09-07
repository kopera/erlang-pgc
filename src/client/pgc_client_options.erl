-module(pgc_client_options).
-moduledoc """
Builds `t:pgc_client:start_options/0` maps, suitable for `pgc:start_link/2,3`.
""".

-export([
    from_uri/1
]).

-define(DEFAULT_PORT, 5432).
-define(SESSION_PARAMETERS, #{
    ~"application_name" => application_name,
    ~"client_encoding" => client_encoding,
    ~"datestyle" => datestyle,
    ~"timezone" => timezone,
    ~"search_path" => search_path,
    ~"options" => options,
    ~"replication" => replication
}).


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
-spec from_uri(unicode:chardata()) -> {ok, pgc_client:start_options()} | {error, Reason} when
    Reason :: {invalid_uri, {atom(), term()}}
            | {invalid_scheme, unicode:unicode_binary()}
            | {invalid_sslmode, unicode:unicode_binary()}
            | {invalid_port | invalid_connect_timeout, unicode:unicode_binary()}
            | {unsupported_parameters, [unicode:unicode_binary()]}
            | missing_user.
from_uri(Uri) ->
    try
        {ok, decode(unicode:characters_to_binary(Uri))}
    catch
        throw:Reason -> {error, Reason}
    end.


decode(Uri) ->
    case uri_string:parse(Uri) of
        {error, Type, Term} ->
            throw({invalid_uri, {Type, Term}});
        #{scheme := Scheme} = Parsed ->
            ok = check_scheme(Scheme),
            options(Parsed)
    end.

check_scheme(Scheme) ->
    case string:lowercase(Scheme) of
        ~"postgresql" -> ok;
        ~"postgres" -> ok;
        _ -> throw({invalid_scheme, Scheme})
    end.


options(Parsed) ->
    Query = maps:from_list(uri_string:dissect_query(maps:get(query, Parsed, ~""))),
    ok = check_supported_parameters(Query),
    {User, Password} = userinfo(maps:get(userinfo, Parsed, undefined)),
    User =/= undefined orelse throw(missing_user),
    Required = #{
        address => address(Parsed, Query),
        user => User,
        database => database(maps:get(path, Parsed, ~""), User)
    },
    Optional = maps:filter(fun (_Key, Value) -> Value =/= undefined end, #{
        password => Password,
        tls => tls(Query),
        connect_timeout => connect_timeout(Query)
    }),
    Options = maps:merge(Required, Optional),
    case session_parameters(Query) of
        Parameters when map_size(Parameters) =:= 0 -> Options;
        Parameters -> Options#{parameters => Parameters}
    end.

check_supported_parameters(Query) ->
    KnownKeys = [~"host", ~"port", ~"sslmode", ~"connect_timeout" | maps:keys(?SESSION_PARAMETERS)],
    case maps:keys(maps:without(KnownKeys, Query)) of
        [] -> ok;
        Unsupported -> throw({unsupported_parameters, Unsupported})
    end.


-doc """
Splits `user[:password]` -- percent-decoding each half only *after* splitting, so a literal `:`
or `@` encoded within the user name or password isn't mistaken for a delimiter.
""".
userinfo(undefined) ->
    {undefined, undefined};
userinfo(UserInfo) ->
    case binary:split(UserInfo, ~":") of
        [User] -> {uri_string:unquote(User), undefined};
        [User, Password] -> {uri_string:unquote(User), uri_string:unquote(Password)}
    end.


-doc """
An empty or `/`-prefixed host (after percent-decoding) is a unix socket directory, per libpq's
own `postgresql://%2Fvar%2Frun%2Fpostgresql/mydb` / `postgresql:///mydb?host=/var/run/postgresql`
conventions -- the latter's `host` query parameter is only consulted when the URI has no
authority host of its own to begin with.
""".
address(Parsed, Query) ->
    Host = case maps:get(host, Parsed, ~"") of
        ~"" -> maps:get(~"host", Query, ~"");
        EncodedHost -> uri_string:unquote(EncodedHost)
    end,
    case Host of
        <<"/", _/binary>> -> #{path => Host};
        ~"" -> #{host => "localhost", port => port(Parsed, Query)};
        _ -> #{host => unicode:characters_to_list(Host), port => port(Parsed, Query)}
    end.

port(Parsed, Query) ->
    case maps:get(port, Parsed, undefined) of
        undefined ->
            case maps:find(~"port", Query) of
                error -> ?DEFAULT_PORT;
                {ok, Text} -> positive_integer(Text, invalid_port)
            end;
        Port -> Port
    end.


database(Path, User) ->
    case uri_string:unquote(Path) of
        ~"" -> User;
        <<"/", Rest/binary>> -> Rest;
        Other -> Other
    end.


tls(Query) ->
    case maps:find(~"sslmode", Query) of
        error -> undefined;
        {ok, ~"disable"} -> disable;
        {ok, Mode} when Mode =:= ~"allow"; Mode =:= ~"prefer" -> prefer;
        % ponytail: `verify-ca`/`verify-full` collapse to plain `require` (encrypted, unverified)
        % since `pgc_client:start_options()`'s `tls` knob has no verify granularity of its own --
        % real certificate verification needs `tls_options` (CA file, hostname check, ...), which
        % this URI parser doesn't accept params for. Add `sslrootcert`/`sslcert`/`sslkey` query
        % parameters, translated into `tls_options`, if verified TLS over a URI is needed.
        {ok, Mode} when Mode =:= ~"require"; Mode =:= ~"verify-ca"; Mode =:= ~"verify-full" -> require;
        {ok, Mode} -> throw({invalid_sslmode, Mode})
    end.


connect_timeout(Query) ->
    case maps:find(~"connect_timeout", Query) of
        error -> undefined;
        {ok, Text} ->
            case positive_integer(Text, invalid_connect_timeout) of
                Seconds when Seconds =< 0 -> infinity;
                % libpq's own rule: values below 2s are bumped up to the 2s minimum rather than
                % rejected.
                Seconds -> max(Seconds, 2) * 1000
            end
    end.

positive_integer(Text, ErrorTag) ->
    case string:to_integer(Text) of
        {Int, ~""} -> Int;
        _ -> throw({ErrorTag, Text})
    end.


session_parameters(Query) ->
    maps:fold(fun (QueryKey, Name, Acc) ->
        case maps:find(QueryKey, Query) of
            {ok, Value} -> Acc#{Name => Value};
            error -> Acc
        end
    end, #{}, ?SESSION_PARAMETERS).


-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

full_uri_test() ->
    ?assertEqual({ok, #{
        address => #{host => "localhost", port => 5433},
        user => ~"user",
        password => ~"p@ss",
        database => ~"mydb",
        tls => require,
        connect_timeout => 10000,
        parameters => #{application_name => ~"my app"}
    }}, pgc_client_options:from_uri(~"postgresql://user:p%40ss@localhost:5433/mydb?sslmode=require&connect_timeout=10&application_name=my%20app")).

default_port_and_database_test() ->
    ?assertEqual({ok, #{
        address => #{host => "localhost", port => 5432},
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
    ?assertEqual({error, missing_user}, pgc_client_options:from_uri(~"postgresql://localhost/mydb")).

invalid_scheme_test() ->
    ?assertEqual({error, {invalid_scheme, ~"mysql"}}, pgc_client_options:from_uri(~"mysql://user@localhost/mydb")).

invalid_sslmode_test() ->
    ?assertEqual({error, {invalid_sslmode, ~"verify-nope"}},
        pgc_client_options:from_uri(~"postgresql://user@localhost/mydb?sslmode=verify-nope")).

unsupported_parameter_test() ->
    ?assertEqual({error, {unsupported_parameters, [~"sslrootcert"]}},
        pgc_client_options:from_uri(~"postgresql://user@localhost/mydb?sslrootcert=/root.crt")).

invalid_uri_test() ->
    ?assertMatch({error, {invalid_uri, _}}, pgc_client_options:from_uri(~"not a uri")).

-endif.
