-module(pgc_notice).
-export_type([
    t/0
]).

-type t() :: #{
    severity := warning | notice | debug | info | log,
    code := binary(),
    message := unicode:unicode_binary(),
    detail => unicode:unicode_binary(),
    hint => unicode:unicode_binary(),
    schema => unicode:unicode_binary(),
    table => unicode:unicode_binary(),
    column => unicode:unicode_binary(),
    data_type => unicode:unicode_binary(),
    constraint => unicode:unicode_binary()
}.