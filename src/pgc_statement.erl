%% @private
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
-type template_identifier() :: {identifier, ref(atom() | unicode:chardata())}.
-type ref(Value) :: #{key => term(), value := Value}.


-spec new(Input) -> t() when
    Input :: unicode:unicode_binary() | {unicode:chardata(), [term()]} | template().
new(Statement) when is_binary(Statement) ->
    {Statement, []};
new({Statement, Parameters}) ->
    {Statement, Parameters};
new(StatementTemplate) when is_list(StatementTemplate) ->
    from_template(StatementTemplate).


%% @private
-spec from_template(template()) -> t().
from_template(Tokens) when is_list(Tokens) ->
    from_template(Tokens, [], [], #{}).


%% @private
from_template([Template | Rest], CommandAcc, ParamsAcc, ParamsMap) when is_list(Template) ->
    from_template(Template ++ Rest, CommandAcc, ParamsAcc, ParamsMap);
from_template([Text | Rest], CommandAcc, ParamsAcc, ParamsMap) when is_binary(Text); is_integer(Text) ->
    from_template(Rest, [Text | CommandAcc], ParamsAcc, ParamsMap);
from_template([{parameter, Param} | Rest], CommandAcc, ParamsAcc, ParamsMap) ->
    {Text, ParamsAcc1, ParamsMap1} = encode_parameter(Param, ParamsAcc, ParamsMap),
    from_template(Rest, [Text | CommandAcc], ParamsAcc1, ParamsMap1);
from_template([{identifier, #{value := Identifier}} | Rest], CommandAcc, ParamsAcc, ParamsMap) ->
    from_template(Rest, [encode_identifier(Identifier) | CommandAcc], ParamsAcc, ParamsMap);
% from_template([{fragment, #{value := Fragment}} | Rest], CommandAcc, ParamsAcc, ParamsMap) ->
%     from_template(Rest, [Fragment | CommandAcc], ParamsAcc, ParamsMap);
from_template([], CommandAcc, ParamsAcc, _ParamsMap) ->
    {lists:reverse(CommandAcc), lists:reverse(ParamsAcc)}.


%% @private
encode_identifier(Name) when is_atom(Name) ->
    encode_identifier(atom_to_binary(Name));
encode_identifier(Name) ->
    [<<"\"">>, string:replace(Name, <<"\"">>, <<"\"\"">>, all), <<"\"">>].


%% @private
encode_parameter(#{key := ParamKey}, ParamsAcc, ParamsMap) when is_map_key(ParamKey, ParamsMap) ->
    #{ParamKey := ParamIndex} = ParamsMap,
    Text = make_placeholder(ParamIndex),
    {Text, ParamsAcc, ParamsMap};
encode_parameter(#{key := ParamKey, value := ParamValue}, ParamsAcc, ParamsMap) ->
    ParamIndex = length(ParamsAcc) + 1,
    Text = make_placeholder(ParamIndex),
    {Text, [ParamValue | ParamsAcc], ParamsMap#{ParamKey => ParamIndex}};
encode_parameter(#{value := ParamValue}, ParamsAcc, ParamsMap) ->
    ParamIndex = length(ParamsAcc) + 1,
    Text = make_placeholder(ParamIndex),
    {Text, [ParamValue | ParamsAcc], ParamsMap}.


%% @private
make_placeholder(Index) ->
    <<$$, (integer_to_binary(Index))/binary>>.
