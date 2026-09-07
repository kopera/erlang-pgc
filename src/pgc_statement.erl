-module(pgc_statement).
-export([
    new/1
]).
-export_type([
    t/0,
    template/0,
    template_identifier/0,
    template_parameter/0
]).


-type t() :: {iodata(), [term()]}.
-type template() :: [unicode:unicode_binary() | byte() | template_parameter() | template_identifier() | template()].
-type template_parameter() :: {parameter, ref(term())}.
-type template_identifier() :: {identifier, ref(atom() | unicode:unicode_binary())}.
-type ref(Value) :: #{key => term(), value := Value}.

-spec new(Input) -> t() when
    Input :: unicode:unicode_binary() | {unicode:chardata(), [term()]} | template().
new(Input) when is_binary(Input) ->
    {Input, []};
new({StatementText, StatementParameters}) ->
    {StatementText, StatementParameters};
new(StatementTemplate) when is_list(StatementTemplate) ->
    from_template(StatementTemplate).

-spec from_template(template()) -> t().
from_template(Tokens) when is_list(Tokens) ->
    from_template(Tokens, [], [], #{}, 0).

-spec from_template(template(), [iodata()], [term()], #{term() => pos_integer()}, non_neg_integer()) -> t().
from_template([Template | Rest], StatementAcc, ParamsAcc, Map, Count) when is_list(Template) ->
    from_template(Template ++ Rest, StatementAcc, ParamsAcc, Map, Count);
from_template([Text | Rest], StatementAcc, ParamsAcc, Map, Count) when is_binary(Text) ->
    from_template(Rest, [Text | StatementAcc], ParamsAcc, Map, Count);
from_template([Byte | Rest], StatementAcc, ParamsAcc, Map, Count) when is_integer(Byte) ->
    % A plain Erlang string ("select 1", as opposed to a ~"..." binary) is just a flat list of
    % these -- template() allows byte() leaves for exactly this case.
    from_template(Rest, [Byte | StatementAcc], ParamsAcc, Map, Count);
from_template([{parameter, Param} | Rest], StatementAcc, ParamsAcc, Map, Count) ->
    {Text, ParamsAcc1, Map1, Count1} = encode_parameter(Param, ParamsAcc, Map, Count),
    from_template(Rest, [Text | StatementAcc], ParamsAcc1, Map1, Count1);
from_template([{identifier, #{value := Identifier}} | Rest], StatementAcc, ParamsAcc, Map, Count) ->
    from_template(Rest, [encode_identifier(Identifier) | StatementAcc], ParamsAcc, Map, Count);
from_template([], StatementAcc, ParamsAcc, _Map, _Count) ->
    {lists:reverse(StatementAcc), lists:reverse(ParamsAcc)}.


-spec encode_identifier(atom() | unicode:unicode_binary()) -> iodata().
encode_identifier(Name) when is_atom(Name) ->
    encode_identifier(atom_to_binary(Name));
encode_identifier(Name) ->
    [<<"\"">>, string:replace(Name, <<"\"">>, <<"\"\"">>, all), <<"\"">>].


-spec encode_parameter(ref(term()), [term()], #{term() => pos_integer()}, non_neg_integer()) ->
    {binary(), [term()], #{term() => pos_integer()}, non_neg_integer()}.
encode_parameter(#{key := ParamKey}, Params, Map, Count) when is_map_key(ParamKey, Map) ->
    #{ParamKey := ParamIndex} = Map,
    {make_placeholder(ParamIndex), Params, Map, Count};
encode_parameter(#{key := ParamKey, value := ParamValue}, Params, Map, Count) ->
    Count1 = Count + 1,
    {make_placeholder(Count1), [ParamValue | Params], Map#{ParamKey => Count1}, Count1};
encode_parameter(#{value := ParamValue}, Params, Map, Count) ->
    Count1 = Count + 1,
    {make_placeholder(Count1), [ParamValue | Params], Map, Count1}.


-spec make_placeholder(pos_integer()) -> binary().
make_placeholder(Index) ->
    <<$$, (integer_to_binary(Index))/binary>>.


-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

new_binary_test() ->
    ?assertEqual({~"SELECT 1", []}, pgc_statement:new(~"SELECT 1")).

new_tuple_test() ->
    ?assertEqual({~"SELECT $1", [123]}, pgc_statement:new({~"SELECT $1", [123]})).

identifier_test() ->
    Template = [
        ~"SELECT * FROM ",
        {identifier, #{value => my_table}},
        ~" WHERE ",
        {identifier, #{value => ~"str_col"}}
    ],
    {Query, Params} = pgc_statement:new(Template),
    ?assertEqual(<<"SELECT * FROM \"my_table\" WHERE \"str_col\"">>, iolist_to_binary(Query)),
    ?assertEqual([], Params).

identifier_escape_test() ->
    Template = [{identifier, #{value => <<"user\"table">>}}],
    {Query, _} = pgc_statement:new(Template),
    %% The double quote should be escaped as ""
    ?assertEqual(<<"\"user\"\"table\"">>, iolist_to_binary(Query)).

positional_parameter_test() ->
    Template = [
        ~"SELECT * FROM users WHERE age > ", {parameter, #{value => 18}},
        ~" AND status = ", {parameter, #{value => ~"active"}}
    ],
    {Query, Params} = pgc_statement:new(Template),
    ?assertEqual(~"SELECT * FROM users WHERE age > $1 AND status = $2", iolist_to_binary(Query)),
    %% Parameters should be strictly in the order they were discovered
    ?assertEqual([18, ~"active"], Params).

keyed_parameter_deduplication_test() ->
    Template = [
        ~"UPDATE users SET failed_logins = ", {parameter, #{key => fails, value => 5}},
        ~", last_login = ", {parameter, #{value => ~"2023-10-01"}},
        ~" WHERE current_fails < ", {parameter, #{key => fails, value => 5}}
    ],
    {Query, Params} = pgc_statement:new(Template),
    ?assertEqual(
        ~"UPDATE users SET failed_logins = $1, last_login = $2 WHERE current_fails < $1",
        iolist_to_binary(Query)
    ),
    %% 'fails' is only inserted into the Params list once, 'last_login' is $2
    ?assertEqual([5, ~"2023-10-01"], Params).

nested_template_list_test() ->
    %% Tests that `Template ++ Rest` was successfully removed and deeply nested
    %% IO lists format correctly without needing flattening.
    Template = [
        ~"SELECT ",
        [
            {identifier, #{value => id}}, ~", ",
            [ {identifier, #{value => name}} ]
        ],
        ~" FROM users WHERE id = ", {parameter, #{value => 42}}
    ],
    {Query, Params} = pgc_statement:new(Template),
    ?assertEqual(<<"SELECT \"id\", \"name\" FROM users WHERE id = $1">>, iolist_to_binary(Query)),
    ?assertEqual([42], Params).

empty_template_test() ->
    ?assertEqual({[], []}, pgc_statement:new([])).

plain_string_test() ->
    %% A plain Erlang string is a bare list of bytes, not a binary -- must not be confused
    %% with a `template()` containing no parameters/identifiers.
    {Query, Params} = pgc_statement:new("SELECT 1"),
    ?assertEqual(<<"SELECT 1">>, iolist_to_binary(Query)),
    ?assertEqual([], Params).

-endif.
