%% @private
-module(pgc_codec_tid).

-behaviour(pgc_codec).
-export([
    init/1,
    encode/2,
    decode/2
]).


init(_Options) ->
    Info = #{
        encodes => [tidsend],
        decodes => [tidrecv]
    },
    {Info, []}.


encode({Block, Tuple}, _Options) when is_integer(Block), Block >= 0, Block =< 4294967295, is_integer(Tuple), Tuple >= 0, Tuple =< 65535 ->
    <<Block:32/integer, Tuple:16/integer>>;
encode(Term, Options) ->
    error(badarg, [Term, Options], [
        {error_info, #{
            module => pgc_codec,
            cause => #{
                1 => "should be a tuple of 2 integers"
            }
        }}
    ]).


decode(<<Block:32/integer, Tuple:16/integer>>, _Options) ->
    {Block, Tuple}.