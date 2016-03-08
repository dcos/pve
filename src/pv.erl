%% @doc Predecessor vector.

-module(pv).
-author("Christopher Meiklejohn <christopher.meiklejohn@gmail.com>").

-export([new/0,
         generate/2,
         dominates/2,
         merge/2,
         learn/2,
         strictly_dominates/2]).

-behaviour(vector).

-dialyzer([{nowarn_function, [strictly_dominates/2, dominates/2]}]).

%% Record specifications.
-record(state, {counter}).

%% Type specifications.
-type actor()   :: atom().
-type count()   :: non_neg_integer().
-type version() :: {actor(), count()}.
-type vector()  :: orddict:orddict(actor(), state()).
-type state()   :: #state{counter :: count()}.

-export_type([actor/0, version/0, vector/0]).

%% Implementation.

%% @doc Generate a new predecessor vector.
-spec new() -> vector().
new() ->
    orddict:new().

%% @doc Generate a predecessor vector and return the object's version.
-spec generate(actor(), vector()) -> {ok, version(), vector()}.
generate(Actor, Vector0) ->
    Counter0 = case orddict:find(Actor, Vector0) of
        {ok, #state{counter=C}} ->
            C;
        _ ->
            0
    end,
    Counter = Counter0 + 1,
    Vector = orddict:store(Actor, #state{counter=Counter}, Vector0),
    {ok, {Actor, Counter}, Vector}.

%% @doc Determine if a vector or an version is dominated by another
%%      vector.
-spec strictly_dominates(version() | vector(), vector()) -> boolean().
strictly_dominates({Actor, Count}, Vector) ->
    case orddict:find(Actor, Vector) of
        {ok, #state{counter=Counter}} ->
            Counter > Count;
        _ ->
            false
    end;
strictly_dominates([], Vector2) when Vector2 =/= [] ->
    true;
strictly_dominates(Vector1, Vector2) ->
    FoldFun = fun(Actor1, #state{counter=Count1}, {Dominates, AtLeastOneStrict}) ->
                      case orddict:find(Actor1, Vector2) of
                          {ok, #state{counter=Count2}} ->
                              {Count2 >= Count1 andalso Dominates,
                               Count2 > Count1 orelse AtLeastOneStrict};
                          error ->
                              {false andalso Dominates,
                               false orelse AtLeastOneStrict}
                      end
              end,
    {Dominates, AtLeastOneStrict} = orddict:fold(FoldFun, {true, false}, Vector1),
    MoreElements = orddict:size(Vector2) > orddict:size(Vector1),
    Dominates andalso (AtLeastOneStrict orelse MoreElements).

%% @doc Determine if a vector or an version is dominated by another
%%      vector.
-spec dominates(version() | vector(), vector()) -> boolean().
dominates({Actor, Count}, Vector) ->
    case orddict:find(Actor, Vector) of
        {ok, #state{counter=Counter}} ->
            Counter >= Count;
        _ ->
            false
    end;
dominates(Vector1, Vector2) ->
    FoldFun = fun(Actor1, #state{counter=Count1}, AccIn) ->
                      case orddict:find(Actor1, Vector2) of
                          {ok, #state{counter=Count2}} ->
                              Count2 >= Count1 andalso AccIn;
                          error ->
                              false andalso AccIn
                      end
              end,
    orddict:fold(FoldFun, true, Vector1).

%% @doc Merge two vectors.
-spec merge(vector(), vector()) -> vector().
merge(Vector1, Vector2) ->
    MergeFun = fun(_Key, #state{counter=Count1}, #state{counter=Count2}) ->
                       #state{counter=max(Count1, Count2)}
               end,
    orddict:merge(MergeFun, Vector1, Vector2).

%% @doc Accumulate knowledge for an version into vector.
-spec learn(version(), vector()) -> vector().
learn({Actor, Count}, Vector) ->
    orddict:store(Actor, #state{counter=Count}, Vector).

%% Internal functions.

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

learn_test() ->
    A0 = new(),

    %% Generate actor a.
    {ok, VersionA1, A1} = generate(a, A0),
    ?assertMatch({a, 1}, VersionA1),

    %% Learn.
    A12 = learn({a, 12}, A1),

    ?assertEqual(orddict:from_list([{a, {state, 12}}]), A12).

merge_test() ->
    A0 = new(),

    %% Generate actor a.
    {ok, VersionA1, A1} = generate(a, A0),
    ?assertMatch({a, 1}, VersionA1),

    %% Generate actor b.
    {ok, VersionB1, B1} = generate(b, A0),
    ?assertMatch({b, 1}, VersionB1),

    A1B1 = merge(A1, B1),
    ?assertEqual(orddict:from_list([{a, {state, 1}}, {b, {state, 1}}]), A1B1).

dominates_test() ->
    A0 = new(),

    %% Generate actor a.
    {ok, VersionA1, A1} = generate(a, A0),
    ?assertMatch({a, 1}, VersionA1),
    ?assertMatch(true, dominates(A0, A1)),

    ?assertMatch(false, dominates(VersionA1, A0)),
    ?assertMatch(true, dominates(VersionA1, A1)),

    %% Generate actor b.
    {ok, VersionB1, B1} = generate(b, A0),
    ?assertMatch({b, 1}, VersionB1),
    ?assertMatch(true, dominates(A0, B1)),
    ?assertMatch(true, strictly_dominates(A0, B1)),
    ?assertMatch(true, dominates(B1, B1)),
    ?assertMatch(false, dominates(A1, B1)),

    {ok, _, A1B2} = generate(b, merge(B1, A1)),
    ?assertMatch(true, strictly_dominates(A1, A1B2)).

-endif.
