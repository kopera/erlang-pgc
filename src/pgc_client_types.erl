-module(pgc_client_types).
-moduledoc false.

-export([
    new/0,
    insert_row/2,
    member/2,
    lookup/2
]).
-export_type([
    t/0
]).

-doc """
One `pg_type` catalog row -- a plain tuple, not a native record: this project's records (OTP 29's
native record support) aren't guaranteed to be plain tagged tuples, so `ets:new/2`'s `{keypos, N}`
(which needs an ordinary positional tuple) doesn't work against one.
""".
-type t() :: {
    Oid :: pgc_protocol:oid(),
    Name :: binary(),
    Kind :: binary(),
    Recv :: binary(),
    Send :: binary(),
    Element :: pgc_protocol:oid() | 0,
    Fields :: [{binary(), pgc_protocol:oid()}]
}.

-spec new() -> ets:table().
new() ->
    ets:new(?MODULE, [protected, {keypos, 1}]).

-doc """
Inserts one row from the type-refresh query's result set (see `pgc_client`'s
`?REFRESH_TYPES_STATEMENT_TEXT`) -- raw wire values, in column order.
""".
-spec insert_row(ets:table(), [null | binary()]) -> ok.
insert_row(Table, [OidText, Name, Kind, Recv, Send, ElementText, FieldsText]) ->
    true = ets:insert(Table, {
        binary_to_integer(non_null(OidText)),
        Name,
        Kind,
        Recv,
        Send,
        binary_to_integer(non_null(ElementText)),
        parse_fields(non_null(FieldsText))
    }),
    ok.

-doc """
`oid`, `typelem` and the `fields` array are never actually null for this query -- narrows the
generic wire-value type down from the type-checker's perspective too, rather than asserting it away.
""".
-spec non_null(null | binary()) -> binary().
non_null(Value) when is_binary(Value) -> Value.

-spec member(ets:table(), pgc_protocol:oid()) -> boolean().
member(Table, Oid) ->
    ets:member(Table, Oid).

-spec lookup(ets:table(), pgc_protocol:oid()) -> {ok, t()} | error.
lookup(Table, Oid) ->
    case ets:lookup(Table, Oid) of
        [Type] -> {ok, Type};
        [] -> error
    end.

% -----------------------------------------------------------------------------
% Helpers
% -----------------------------------------------------------------------------

-doc """
Parses the `fields` column: a Postgres array literal of `"name:oid"` entries (e.g.
`{id:23,name:25}`), empty (`{}`) for anything that isn't a composite type.
""".
-spec parse_fields(binary()) -> [{binary(), pgc_protocol:oid()}].
parse_fields(~"{}") ->
    [];
parse_fields(Text) ->
    Inner = binary:part(Text, 1, byte_size(Text) - 2),
    [begin
        [Name, OidText] = binary:split(Entry, ~":"),
        {Name, binary_to_integer(OidText)}
    end || Entry <- binary:split(Inner, ~",", [global])].
