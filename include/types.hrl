-record(pgsql_type_info, {
    oid :: pgsql_types:oid(),
    name :: binary(),
    send :: binary(),
    recv :: binary(),
    element :: pgsql_types:oid() | undefined,
    parent :: pgsql_types:oid() | undefined,
    fields :: [{binary(), pgsql_types:oid()}] | undefined
}).
