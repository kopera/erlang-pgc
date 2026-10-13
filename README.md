# pgc

PostgreSQL client for Erlang, with connection pooling, prepared statement caching and binary encoding of
parameters and results.

pgc requires Erlang/OTP 29. Its tests run against PostgreSQL 18.

## Installation

Add pgc to the dependencies in `rebar.config`:

```erlang
{deps, [
    pgc
]}.
```

Then add it to the `applications` of your `.app.src`.

## Usage

`pgc` runs statements on a pool of connections:

```erlang
{ok, ClientOptions} = pgc_client_options:from_uri(~"postgresql://postgres:postgres@localhost/postgres"),
PoolOptions = #{max_size => 10},
{ok, Pool} = pgc:start_link(ClientOptions, PoolOptions),
{ok, #{command := ~"select", rows := 1}, [#{~"answer" := 42}]} =
    pgc:execute(Pool, {~"select $1::int4 as answer", [42]}).
```

A transaction holds one connection. Its statements run on the transaction reference, and the fun returns
`{commit, Result}` or `{rollback, Result}`:

```erlang
Name = pgc:transaction(Pool, fun (Transaction) ->
    {ok, _, [#{~"name" := Name}]} = pgc:execute(Transaction, {~"select $1::text as name", [~"pgc"]}),
    {commit, Name}
end).
```

Under a supervisor, start the pool with `pgc:child_spec/3,4`.

Rows are maps keyed by column name. The `row` option returns them as lists, tuples or proplists instead:

```erlang
{ok, _, [{42, ~"pgc"}]} = pgc:execute(Pool, ~"select 42 as answer, 'pgc' as name", #{row => tuple}).
```

`pgc_client` is a single connection, with `execute_fold/6` to process rows as they arrive. `pgc_statement`
builds statements from templates with named parameters and quoted identifiers.

## Connection options

`pgc_client_options:from_uri/1` reads a `postgresql://` URI with these query parameters:

- `host` and `port`, when the URI has no host of its own. A host starting with `/` is the directory of the
  server's Unix socket.
- `sslmode`: `disable`, `allow`, `prefer` (the default), `require` or `verify-full`.
- `connect_timeout`, in seconds.
- `keepalives` (`0` or `1`) and `keepalives_idle`, in seconds: how often an idle connection pings the server.
  The default is every 30 seconds.
- The session parameters `application_name`, `client_encoding`, `datestyle`, `timezone`, `search_path` and
  `options`.

Other parameters are rejected.

`verify-full` checks the server certificate against the system's CA certificates and its host name, wildcard
certificates included. pgc sends the host name as SNI unless the host is an IP address. Known limits:

- With an IP address as host, `verify-full` does not check the certificate's name.
- A certificate without a subjectAltName fails `verify-full`. pgc does not fall back to its CN.
- `verify-ca` is rejected.

## Data representation

`null` is SQL `NULL` in both directions.

| PostgreSQL | Erlang |
| --- | --- |
| `bool` | `true`, `false` |
| `int2`, `int4`, `int8`, `oid` and other `oid` types, `xid8` | integer |
| `float4`, `float8` | float, `'NaN'`, `infinity`, `'-infinity'` |
| `numeric` | binary, the decimal text as PostgreSQL prints it, `'NaN'`, `infinity`, `'-infinity'` |
| `text`, `varchar`, `char(n)`, `name`, `citext` | binary |
| `"char"` | integer, the byte |
| `bytea` | binary |
| `uuid` | 16-byte binary |
| `inet` | IP address tuple for a host, `{Address, PrefixLength}` for a network |
| `cidr` | `{Address, PrefixLength}` |
| `date` | `{Year, Month, Day}` |
| `time` | integer, time since midnight in native time units |
| `timestamp`, `timestamptz` | integer, system time in native time units, `infinity`, `'-infinity'` |
| `interval` | `#{months => M, days => D, microseconds => U}` |
| `json`, `jsonb` | decoded with OTP's `json` module |
| enum types | binary |
| arrays | list, nested for more dimensions |
| ranges | `empty` or `#{lower => Bound, upper => Bound}`, a bound being `unbound`, `{inclusive, V}` or `{exclusive, V}` |
| multiranges | list of ranges |
| composite types | map keyed by field name, or by position (1, 2, …) for an anonymous `row(…)` |
| `bit`, `varbit` | bitstring |
| `tid` | `{Block, Offset}` |
| `hstore` | map of binaries, values may be `null` |
| `ltree`, `lquery` | binary |
| `void` | `undefined` |

A `numeric` is sent as an integer, a float, or decimal text as a binary or string, with an optional exponent
(`<<"1.5e3">>`).

A one-dimensional array is sent as a list. An array of more dimensions is sent as `{array, Dimensions, Nested}`,
for example `{array, 2, [[1, 2], [3, 4]]}`.

`money`, `macaddr`, the geometric types, `xml` and `tsvector` are not supported yet. Reading or sending
them fails with `{pgc, {missing_decoder, _}}` or `{pgc, {missing_encoder, _}}`.

The `codec` option of `execute` changes some representations for one call:

```erlang
{ok, _, [#{~"at" := {{2026, 10, 13}, {12, 0, 0}}}]} =
    pgc:execute(Pool, ~"select '2026-10-13 12:00:00'::timestamp as at", #{
        codec => #{timestamp => #{representation => {calendar, datetime}}}
    }).
```

- `timestamp => #{representation => R}`: `{calendar, datetime}` or `{system_time, Unit}`.
- `time => #{representation => R}`: `{calendar, time}` or `{system_time, Unit}`.
- `numeric => #{representation => R}`: `binary` or `float`.
- `enum => #{decode => D}`: `binary`, `atom` or `existing_atom`.
- `json => #{codec => Module}`: a module with `encode/1` and `decode/1`, used instead of `json`.

## License

Apache License 2.0, see `LICENSE.md`.
