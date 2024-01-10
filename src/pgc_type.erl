-module(pgc_type).
-export_type([
    t/0,
    oid/0
]).

-include("./pgc_type.hrl").

-type t() :: #pgc_type{}.
-type oid() :: 1..4294967295.