-record(pgsql_type_info, {
    oid :: pgsql_types:oid(),
    name :: binary(),
    send :: binary(),
    recv :: binary(),
    element :: pgsql_types:oid() | undefined,
    parent :: pgsql_types:oid() | undefined,
    fields :: [{binary(), pgsql_types:oid()}] | undefined
}).

-record(pgsql_interval, {
    months :: non_neg_integer(),
    days :: non_neg_integer(),
    micro_seconds :: non_neg_integer()
}).

-record(pgsql_date, {
    year :: non_neg_integer(),
    month :: 1..12,
    day :: 1..31
}).

-record(pgsql_time, {
    hours :: 0..23,
    minutes :: 0..59,
    seconds :: 0..59,
    micro_seconds :: 0..999999
}).
