-ifndef(PGC_STATEMENT_HRL).
-define(PGC_STATEMENT_HRL, true).

-include("./pgc_row.hrl").

-record(pgc_statement, {
    name :: binary(),
    % text :: unicode:unicode_binary(),
    hash :: binary(),
    parameters :: [pgc_type:oid()],
    result :: [#pgc_row_field{}]
}).

-endif.