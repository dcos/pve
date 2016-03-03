%% @doc Predecessor vector.

-module(pv).
-author("Christopher Meiklejohn <christopher.meiklejohn@gmail.com>").

-export([new/0,
         increment/2,
         dominates/2,
         merge/2,
         aggregate/2,
         strict_dominates/2]).

-behaviour(vector).

-include("vector.hrl").

-dialyzer([{nowarn_function, [compare/3]}, no_improper_lists]).

%% Record specifications.
-record(state, {counter}).

%% Type specifications.
-type actor()   :: atom().
-type count()   :: non_neg_integer().
-type update()  :: {actor(), count()}.
-type vector()  :: orddict:orddict(actor(), state()).
-type state()   :: #state{counter :: count()}.

-export_type([actor/0, update/0, vector/0]).

%% Implementation.

%% @doc Generate a new predecessor vector.
-spec new() -> vector().
new() ->
    orddict:new().

%% @doc Increment a predecessor vector and return the object's version.
-spec increment(actor(), vector()) -> {ok, update(), vector()}.
increment(Actor, Vector0) ->
    Counter0 = case orddict:find(Actor, Vector0) of
        {ok, #state{counter=C}} ->
            C;
        _ ->
            0
    end,
    Counter = Counter0 + 1,
    Vector = orddict:store(Actor, #state{counter=Counter}, Vector0),
    {ok, {Actor, Counter}, Vector}.

%% @doc Determine if `Vector1' strictly dominates `Vector2'.
-spec strict_dominates(vector(), vector()) -> boolean().
strict_dominates(Vector1, Vector2) ->
    Comparator = fun(Value1, Value2) ->
                     Value1 > Value2
                 end,
    compare(Comparator, Vector1, Vector2).

%% @doc Determine if `Vector1' dominates `Vector2'.
-spec dominates(vector(), vector()) -> boolean().
dominates(Vector1, Vector2) ->
    Comparator = fun(Value1, Value2) ->
                     Value1 >= Value2
                 end,
    compare(Comparator, Vector1, Vector2).

%% @doc Merge two vectors.
-spec merge(vector(), vector()) -> vector().
merge(Vector1, Vector2) ->
    MergeFun = fun(_Key, Value1, Value2) ->
                       max(Value1, Value2)
               end,
    orddict:merge(MergeFun, Vector1, Vector2).

%% @doc Accumulate knowledge for an update into vector.
-spec aggregate(update(), vector()) -> vector().
aggregate({Actor, Count}, Vector) ->
    orddict:store(Actor, Count, Vector).

%% Internal functions.

%% @private
compare(Comparator, Vector1, Vector2) ->
    FoldFun = fun(Actor2, Count2, AccIn) ->
                      case orddict:find(Actor2, Vector1) of
                          {ok, Count1} ->
                              Comparator(Count1, Count2) andalso AccIn;
                          error ->
                              false andalso AccIn
                      end
              end,
    orddict:fold(FoldFun, true, Vector2).


-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

merge_test() ->
    A0 = new(),

    %% Increment actor a.
    {ok, UpdateA1, A1} = increment(a, A0),
    ?assertMatch({a, 1}, UpdateA1),

    %% Increment actor b.
    {ok, UpdateB1, B1} = increment(b, A0),
    ?assertMatch({b, 1}, UpdateB1),

    A1B1 = merge(A1, B1),
    ?assertEqual(orddict:from_list([{a, {state, 1}}, {b, {state, 1}}]), A1B1).

dominates_test() ->
    A0 = new(),

    %% Increment actor a.
    {ok, UpdateA1, A1} = increment(a, A0),
    ?assertMatch({a, 1}, UpdateA1),
    ?assertMatch(true, dominates(A1, A0)),

    %% Increment actor b.
    {ok, UpdateB1, B1} = increment(b, A0),
    ?assertMatch({b, 1}, UpdateB1),
    ?assertMatch(true, dominates(B1, A0)),
    ?assertMatch(true, strict_dominates(B1, A0)),
    ?assertMatch(true, dominates(B1, B1)),
    ?assertMatch(false, dominates(B1, A1)).

-endif.
