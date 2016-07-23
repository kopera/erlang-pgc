-record(pgsql_type_info, {
    oid :: pgsql_types:oid(),
    name :: atom(),
    send :: atom(),
    recv :: atom(),
    element :: pgsql_types:oid() | undefined,
    parent :: pgsql_types:oid() | undefined,
    fields :: [{atom(), pgsql_types:oid()}] | undefined
}).

-record(pgsql_interval, {
    months :: non_neg_integer(),
    days :: non_neg_integer(),
    micro_seconds :: non_neg_integer()
}).

-record(pgsql_date, {
    year :: 1..5874897,
    month :: 1..12,
    day :: 1..31
}).

-record(pgsql_time, {
    hours :: 0..23,
    minutes :: 0..59,
    seconds :: 0..59,
    micro_seconds :: 0..999999
}).

-record(pgsql_datetime, {
    year :: 1..294276,
    month :: 1..12,
    day :: 1..31,
    hours :: 0..23,
    minutes :: 0..59,
    seconds :: 0..59,
    micro_seconds :: 0..999999
}).

-record(pgsql_macaddr, {
    address :: {byte(), byte(), byte(), byte(), byte(), byte()}
}).

-record(pgsql_inet, {
    address :: inet:ip_address()
}).

-record(pgsql_cidr, {
    address :: inet:ip_address(),
    mask :: byte()
}).
