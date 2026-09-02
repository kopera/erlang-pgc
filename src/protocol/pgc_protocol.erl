-module(pgc_protocol).
-export([
    from_error_response_fields/1
]).
-export_record([
    error
]).
-export_type([
    error/0
]).


-record #error{
    severity :: panic | fatal | error,
    code :: binary(),
    message :: unicode:unicode_binary(),

    info :: # {
        detail => unicode:unicode_binary(),
        hint => unicode:unicode_binary(),
        position => pos_integer(),
        internal_position => pos_integer(),
        internal_query => unicode:unicode_binary(),
        where => [unicode:unicode_binary()],
        schema => unicode:unicode_binary(),
        table => unicode:unicode_binary(),
        column => unicode:unicode_binary(),
        data_type => unicode:unicode_binary(),
        constraint => unicode:unicode_binary(),
        file => unicode:unicode_binary(),
        line => non_neg_integer(),
        routine => unicode:unicode_binary()
    }
}.
-type error() :: #error{}.

-doc false.
-spec from_error_response_fields(pgc_protocol_message:error_response_fields()) -> error().
from_error_response_fields(#{severity := Severity, code := Code, message := Message} = Fields) ->
    #error{
        severity = Severity,
        code = Code,
        message = Message,

        info = maps:without([severity, code, message], Fields)
    }.
