-module(pgc_protocol).
-moduledoc false.
-export_type([
    oid/0
]).

-type oid() :: 1..4294967295.
