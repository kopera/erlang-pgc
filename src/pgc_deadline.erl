-module(pgc_deadline).
-export([
    from_timeout/1,
    remaining/1
]).
-export_type([
    t/0
]).

-record(deadline, {
    value :: pos_integer() | infinity
}).
-opaque t() :: #deadline{}.



%% @doc create a new deadline for a given timeout.
-spec from_timeout(timeout()) -> t().
from_timeout(Timeout) ->
    from_timeout(Timeout, millisecond).

%% @private
from_timeout(infinity, _Unit) ->
    #deadline{value = infinity};

from_timeout(Timeout, Unit) ->
    Delta = erlang:convert_time_unit(Timeout, Unit, native),
    #deadline{value = erlang:monotonic_time() + Delta}.


%% @doc return the remaining milliseconds to deadline or infinity.
-spec remaining(t()) -> non_neg_integer() | infinity.
remaining(Timeout) ->
    remaining(Timeout, millisecond).

%% @private
remaining(#deadline{value = infinity}, _Unit) ->
    infinity;
remaining(#deadline{value = Value}, Unit) when is_integer(Value) ->
    case Value - erlang:monotonic_time() of
        Remaining when Remaining > 0 ->
            erlang:convert_time_unit(Remaining, native, Unit);
        _ -> 0
    end.

