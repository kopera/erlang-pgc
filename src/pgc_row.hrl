-ifndef(PGC_ROW_HRL).
-define(PGC_ROW_HRL, true).

-record(pgc_row_field, {
    name :: binary(),
    table_oid :: pgc_type:oid() | 0,
    field_number :: pos_integer() | 0,
    type_oid :: pgc_type:oid(),
    type_size :: integer(),
    type_modifier :: integer(),
    format :: text | binary
}).

-endif.