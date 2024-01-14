-ifndef(PGC_TYPE_HRL).
-define(PGC_TYPE_HRL, true).

-record(pgc_type, {
    oid :: pgc_type:oid(),
    namespace :: atom(),
    name :: atom(),
    type :: base | composite | domain | enum | pseudo | range | multirange | other,
    send :: atom(),
    recv :: atom(),
    element :: pgc_type:oid() | undefined,
    parent :: pgc_type:oid() | undefined,
    fields :: [{atom(), pgc_type:oid()}] | undefined
}).

-endif.