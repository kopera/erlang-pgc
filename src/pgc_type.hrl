-record(pgc_type, {
    oid :: pgc_type:oid(),
    name :: atom(),
    type :: base | composite | domain | enum | pseudo | range | multirange | other,
    send :: atom(),
    recv :: atom(),
    element :: pgc_type:oid() | undefined,
    parent :: pgc_type:oid() | undefined,
    fields :: [{atom(), pgc_type:oid()}] | undefined
}).