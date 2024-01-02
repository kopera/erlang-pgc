-module(pgc_type).
-export_type([
    oid/0
]).

-type oid() :: 1..4294967295.