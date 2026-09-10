-module(pgc_statement).
-export([
    new/1
]).
-export([
    text/1,
    parameters/1
]).
-export_type([
    t/0,
    template/0,
    template_identifier/0,
    template_parameter/0
]).


-record #statement{
    text :: iodata(),
    parameters :: [term()]
}.

-type t() :: #statement{}.
-type template() :: [unicode:unicode_binary() | byte() | template_parameter() | template_identifier() | template()].

-type template_parameter() :: {parameter, template_parameter_info()}.
-type template_parameter_info() :: #{key => term(), type => atom() | unicode:unicode_binary() | string(), value := term()}.

-type template_identifier() :: {identifier, template_identifier_info()}.
-type template_identifier_info() :: #{value := atom() | unicode:unicode_binary()}.


-spec new(Input) -> t() when
    Input :: unicode:unicode_binary() | {unicode:chardata(), [term()]} | t() | template().
new(Input) when is_binary(Input) ->
    #statement{text = Input, parameters = []};
new({Text, Parameters}) ->
    #statement{text = to_string(Text), parameters = Parameters};
new(#statement{} = Statement) ->
    Statement;
new(StatementTemplate) when is_list(StatementTemplate) ->
    from_template(StatementTemplate).


-doc "Return the statement text".
-spec text(t()) -> iodata().
text(#statement{text = Text}) ->
    Text.

-doc "Return the statement parameters".
-spec parameters(t()) -> [term()].
parameters(#statement{parameters = Parameters}) ->
    Parameters.


-spec from_template(template()) -> t().
from_template(Tokens) when is_list(Tokens) ->
    from_template(Tokens, [], [], #{}).

-spec from_template(Template, TextAcc, ParamsAcc, UsedParamsAcc) -> t() when
    Template :: template(),
    TextAcc :: [iodata()],
    ParamsAcc :: [term()],
    UsedParamsAcc :: #{term() => pos_integer()}.
from_template([Text | Rest], TextAcc, ParamsAcc, UsedParamsAcc) when is_binary(Text) ->
    from_template(Rest, [Text | TextAcc], ParamsAcc, UsedParamsAcc);
from_template([Template | Rest], TextAcc, ParamsAcc, UsedParamsAcc) when is_list(Template) ->
    from_template(Template ++ Rest, TextAcc, ParamsAcc, UsedParamsAcc);
from_template([Byte | _Rest] = Template, TextAcc, ParamsAcc, UsedParamsAcc) when is_integer(Byte)  ->
    {Text, Rest} = lists:splitwith(fun (Token) -> is_integer(Token) orelse is_binary(Token) end, Template),
    % eqwalizer:ignore splitwith is not properly typed
    from_template(Rest, [to_string(Text) | TextAcc], ParamsAcc, UsedParamsAcc);
from_template([{parameter, ParameterInfo} | Rest], TextAcc, ParamsAcc, UsedParamsAcc) ->
    {Text, ParamsAcc1, UsedParamsAcc1} = encode_parameter(ParameterInfo, ParamsAcc, UsedParamsAcc),
    from_template(Rest, [Text | TextAcc], ParamsAcc1, UsedParamsAcc1);
from_template([{identifier, IndentifierInfo} | Rest], TextAcc, ParamsAcc, UsedParamsAcc) ->
    from_template(Rest, [encode_identifier(IndentifierInfo) | TextAcc], ParamsAcc, UsedParamsAcc);
from_template([], TextAcc, ParamsAcc, _UsedParamsAcc) ->
    #statement{text = lists:reverse(TextAcc), parameters = lists:reverse(ParamsAcc)}.


-spec encode_identifier(template_identifier_info()) -> iodata().
encode_identifier(#{value := Value}) ->
    [<<"\"">>, string:replace(to_string(Value), <<"\"">>, <<"\"\"">>, all), <<"\"">>].


-spec encode_parameter(Info, ParamsAcc, UsedParamsAcc) -> {iodata(), ParamsAcc, UsedParamsAcc} when
    Info :: template_parameter_info(),
    ParamsAcc :: [term()],
    UsedParamsAcc :: #{term() => pos_integer()}.
encode_parameter(#{key := ParamKey, value := ParamValue} = ParamInfo, ParamsAcc, UsedParamsAcc) ->
    case UsedParamsAcc of
        #{ParamKey := ParamIndex} ->
            {make_placeholder(ParamIndex), ParamsAcc, UsedParamsAcc};
        #{} ->
            ParamIndex = length(ParamsAcc) + 1,
            {make_placeholder(ParamInfo, ParamIndex), [ParamValue | ParamsAcc], UsedParamsAcc#{ParamKey => ParamIndex}}
    end;
encode_parameter(#{value := ParamValue} = ParamInfo, ParamsAcc, UsedParamsAcc) ->
    ParamIndex = length(ParamsAcc) + 1,
    {make_placeholder(ParamInfo, ParamIndex), [ParamValue | ParamsAcc], UsedParamsAcc}.


-spec make_placeholder(template_parameter_info(), pos_integer()) -> iodata().
make_placeholder(#{type := Type}, ParamIndex) ->
    <<"(", (make_placeholder(ParamIndex))/binary , "::", (to_string(Type))/binary, ")">>;
make_placeholder(#{}, ParamIndex) ->
    make_placeholder(ParamIndex).

-spec make_placeholder(pos_integer()) -> binary().
make_placeholder(ParamIndex) ->
    <<$$, (integer_to_binary(ParamIndex))/binary>>.


-spec to_string(atom() | unicode:chardata()) -> unicode:unicode_binary().
to_string(Value) when is_atom(Value) ->
    atom_to_binary(Value);
to_string(Value) ->
    pgc_string:characters_to_binary(Value).


-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

new_binary_test() ->
    ?assertEqual(#statement{text = ~"SELECT 1", parameters = []}, pgc_statement:new(~"SELECT 1")).

new_tuple_test() ->
    ?assertEqual(#statement{text = ~"SELECT $1", parameters = [123]}, pgc_statement:new(#statement{text = ~"SELECT $1", parameters = [123]})).

identifier_test() ->
    Template = [
        ~"SELECT * FROM ",
        {identifier, #{value => my_table}},
        ~" WHERE ",
        {identifier, #{value => ~"str_col"}}
    ],
    #statement{text = Query, parameters = Params} = pgc_statement:new(Template),
    ?assertEqual(<<"SELECT * FROM \"my_table\" WHERE \"str_col\"">>, iolist_to_binary(Query)),
    ?assertEqual([], Params).

identifier_escape_test() ->
    Template = [{identifier, #{value => <<"user\"table">>}}],
    #statement{text = Query, parameters = _} = pgc_statement:new(Template),
    %% The double quote should be escaped as ""
    ?assertEqual(<<"\"user\"\"table\"">>, iolist_to_binary(Query)).

positional_parameter_test() ->
    Template = [
        ~"SELECT * FROM users WHERE age > ", {parameter, #{value => 18}},
        ~" AND status = ", {parameter, #{value => ~"active"}}
    ],
    #statement{text = Query, parameters = Params} = pgc_statement:new(Template),
    ?assertEqual(~"SELECT * FROM users WHERE age > $1 AND status = $2", iolist_to_binary(Query)),
    %% Parameters should be strictly in the order they were discovered
    ?assertEqual([18, ~"active"], Params).

keyed_parameter_deduplication_test() ->
    Template = [
        ~"UPDATE users SET failed_logins = ", {parameter, #{key => fails, value => 5}},
        ~", last_login = ", {parameter, #{value => ~"2023-10-01"}},
        ~" WHERE current_fails < ", {parameter, #{key => fails, value => 5}}
    ],
    #statement{text = Query, parameters = Params} = pgc_statement:new(Template),
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
    #statement{text = Query, parameters = Params} = pgc_statement:new(Template),
    ?assertEqual(<<"SELECT \"id\", \"name\" FROM users WHERE id = $1">>, iolist_to_binary(Query)),
    ?assertEqual([42], Params).

empty_template_test() ->
    ?assertEqual(#statement{text = [], parameters = []}, pgc_statement:new([])).

plain_string_test() ->
    %% A plain Erlang string is a bare list of bytes, not a binary -- must not be confused
    %% with a `template()` containing no parameters/identifiers.
    #statement{text = Query, parameters = Params} = pgc_statement:new("SELECT 1"),
    ?assertEqual(<<"SELECT 1">>, iolist_to_binary(Query)),
    ?assertEqual([], Params).

-endif.
