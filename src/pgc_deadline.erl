%% @private
-module(pgc_deadline).
-export([
    from_timeout/1,
    to_timeout/1,
    to_abs_timeout/1
]).
-export([
    start_timer/3,
    cancel_timer/1
]).
-export_type([
    t/0,
    timer/1
]).

-type t() :: integer() | infinity.


%% @doc create a new deadline for a given timeout.
-spec from_timeout(timeout() | {abs, integer()}) -> t().
from_timeout({abs, Deadline}) when is_integer(Deadline) ->
    Deadline;
from_timeout(infinity) ->
    infinity;
from_timeout(Timeout) when is_integer(Timeout), Timeout >= 0 ->
    Delta = erlang:convert_time_unit(Timeout, millisecond, native),
    erlang:monotonic_time() + Delta.


-spec to_timeout(t()) -> infinity | non_neg_integer() | infinity.
to_timeout(infinity) ->
    infinity;
to_timeout(Deadline) ->
    case Deadline - erlang:monotonic_time() of
        Remaining when Remaining > 0 ->
            erlang:convert_time_unit(Remaining, native, millisecond);
        _ -> 0
    end.


-spec to_abs_timeout(t()) -> infinity | {abs, integer()}.
to_abs_timeout(infinity) ->
    infinity;
to_abs_timeout(Deadline) ->
    Milliseconds = erlang:convert_time_unit(Deadline, native, millisecond),
    {abs, Milliseconds}.


-spec start_timer(Dest :: pid(), Message, Deadline :: t()) -> timer(Message).
-opaque timer(Message) :: infinity | {reference(), Message}.
start_timer(_Dest, _Message, infinity) ->
    infinity;
start_timer(Dest, Message, Deadline) ->
    Milliseconds = erlang:convert_time_unit(Deadline, native, millisecond),
    {erlang:send_after(Milliseconds, Dest, Message, [{abs, true}]), Message}.


-spec cancel_timer(timer(term())) -> ok.
cancel_timer(infinity) ->
    ok;
cancel_timer({TimerRef, Message}) ->
    case erlang:cancel_timer(TimerRef) of
        false ->
            receive
                Message -> ok
            after 0 -> ok
            end;
        _ ->
            ok
    end.