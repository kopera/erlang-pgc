-include("../protocol/pgc_message.hrl").

% ------------------------------------------------------------------------------
% Internal records
% ------------------------------------------------------------------------------

-record(statement, {
    name :: binary(),
    % text :: unicode:unicode_binary(),
    hash :: binary(),
    parameters :: [pgc_type:oid()],
    result :: [#pgc_row_field{}]
}).

% ------------------------------------------------------------------------------
% Data records
% ------------------------------------------------------------------------------

-record(connection, {
    transport :: pgc_transport:t(),
    transport_tags :: pgc_transport:tags(),
    transport_buffer :: binary(),

    ping_interval :: timeout(),
    ping_timeout :: timeout(),

    key :: {pos_integer(), pos_integer()} | undefined,
    parameters :: #{atom() => iodata()},

    types :: pgc_types:t(),
    statements :: #{unicode:unicode_binary() => #statement{}},

    codecs :: pgc_codecs:t()
}).


% ------------------------------------------------------------------------------
% State records
% ------------------------------------------------------------------------------

-record(disconnected, {
}).

-record(authenticating, {
    username :: unicode:unicode_binary(),
    database :: unicode:unicode_binary(),
    parameters :: #{atom() => iodata()},
    auth_state :: pgc_auth:state()
}).


-record(configuring, {
}).


-record(initializing, {
}).

-record(ready, {
    status :: idle | transaction | error
}).


-record(preparing, {
    from :: gen_statem:from(),

    statement_name :: unicode:unicode_binary(),
    statement_text :: unicode:chardata(),
    statement_text_hash :: binary(),
    statement_parameters_types :: [pgc_type:oid()]
}).


-record(unpreparing, {
    statement_name :: unicode:unicode_binary()
}).

-record(executing, {
    client_ref :: pid() | reference(),
    client_monitor :: reference(),

    execution_ref :: reference(),

    statement_name :: unicode:unicode_binary(),
    statement_parameters :: [{text | binary, iodata()}],
    statement_result_format :: nonempty_list(text | binary),
    statement_result_row_decoder :: fun(([null | binary()]) -> {ok, term()} | {error, pgc_error:t()})
}).

-record(executing_simple, {
    from :: gen_statem:from() | self,

    statement_text :: unicode:chardata()
}).

-record(refreshing_types, {
    missing :: [pgc_type:oid()]
}).

-record(canceling, {
}).

-record(syncing, {
    timeout :: timeout()
}).

-record(stopping, {
    reason :: normal | pgc_error:t()
}).


-define(default_ping_interval, 5000).
-define(default_hibernate_after, ?default_ping_interval div 2).


-define(internal_statement_name_prefix, "_pgc_connection_:").
-define(refresh_types_statement_name, <<?internal_statement_name_prefix, "refresh_types">>).
-define(refresh_types_statement_text, <<
"select
    pg_type.oid as oid,
    pg_namespace.nspname as namespace,
    pg_type.typname as name,
    pg_type.typtype as type,
    pg_type.typsend as send,
    pg_type.typreceive as recv,
    pg_type.typelem as element_type,
    coalesce(pg_range.rngsubtype, 0) as parent_type,
    array (
        select pg_attribute.attname
        from pg_attribute
        where pg_attribute.attrelid = pg_type.typrelid
          and pg_attribute.attnum > 0
          and not pg_attribute.attisdropped
        order by pg_attribute.attnum
    ) as fields_names,
    array (
        select pg_attribute.atttypid
        from pg_attribute
        where pg_attribute.attrelid = pg_type.typrelid
          and pg_attribute.attnum > 0
          and not pg_attribute.attisdropped
        order by pg_attribute.attnum
    ) as fields_types
from pg_catalog.pg_type
  left join pg_catalog.pg_range on pg_range.rngtypid = pg_type.oid
  left join pg_catalog.pg_namespace on pg_namespace.oid = pg_type.typnamespace"
>>).