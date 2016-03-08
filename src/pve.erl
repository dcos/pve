%% @doc Predecessor vector with negative exceptions.

-module(pve).
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
-record(state, {counter, exceptions = []}).

%% Type specifications.
-type actor()     :: atom().
-type count()     :: non_neg_integer().
-type version()   :: {actor(), count()}.
-type exception() :: non_neg_integer().
-type vector()    :: orddict:orddict(actor(), state()).
-type state()     :: #state{counter :: count(), exceptions :: [exception()]}.

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
        {ok, #state{counter=Counter, exceptions=Exceptions}} ->
            Counter >= Count andalso not lists:member(Count, Exceptions);
        _ ->
            false
    end;
dominates(Vector1, Vector2) ->
    FoldFun = fun(Actor1, #state{counter=Count1, exceptions=Exceptions1}, AccIn) ->
                      case orddict:find(Actor1, Vector2) of
                          {ok, #state{counter=Count2, exceptions=Exceptions2}} ->
                              dominates(Actor1,
                                        Vector1, Count1, Exceptions1,
                                        Vector2, Count2, Exceptions2) andalso AccIn;
                          error ->
                              false andalso AccIn
                      end
              end,
    orddict:fold(FoldFun, true, Vector1).

%% From the paper: ``a predecessors vector with exceptions X dominates
%% another vector Y if the respective PVs without the exceptions
%% dominate, and no exception included in X is dominated by Y.''
%%
%% This subroutine is performed for *each* actor, called from
%% @ref{dominate/2}.
%%
dominates(Actor, YVector, YCount, _YExceptions, _XVector, XCount, XExceptions) ->
    %% Does the predecessor vector dominate?
    Dominated = XCount >= YCount,

    %% Does Y dominate any of X's exceptions?
    DominatedExceptions = lists:any(fun(E) ->
                                            dominates({Actor, E}, YVector)
                                    end, XExceptions),

    Dominated andalso not DominatedExceptions.

%% @doc Merge two vectors.
-spec merge(vector(), vector()) -> vector().
merge(Vector1, Vector2) ->
    MergeFun = fun(Key, #state{counter=Count1, exceptions=Exceptions1},
                         #state{counter=Count2, exceptions=Exceptions2}) ->

                       %% Take the max of the counter.
                       Max = max(Count1, Count2),

                       %% Exceptions from Vector1 continue being
                       %% exceptions if they aren't dominated by Vector2.
                       E1 = lists:filter(fun(Version) ->
                                            not dominates({Key, Version}, Vector2)
                                    end, Exceptions1),

                       %% Same, the other direction.
                       E2 = lists:filter(fun(Version) ->
                                            not dominates({Key, Version}, Vector1)
                                    end, Exceptions2),

                       %% Merge.
                       Exceptions = lists:usort(E1 ++ E2),

                       #state{counter=Max, exceptions=Exceptions}
               end,
    orddict:merge(MergeFun, Vector1, Vector2).

%% @doc Accumulate knowledge for an version into vector.
-spec learn(version(), vector()) -> vector().
learn({Actor, Count}, Vector) ->
    {Counter0, Exceptions0} = case orddict:find(Actor, Vector) of
        {ok, #state{counter=C, exceptions=E}} ->
            {C, E};
        _ ->
            {0, []}
    end,

    %% Determine new max counter value.
    Max = max(Count, Counter0),

    %% Generate possible new exceptions.
    NewExceptions = case Max =< Counter0 of
        true ->
            [];
        false ->
            %% Generate a list of exceptions up to the
            %% new value for the maximum.
            lists:seq(Counter0 + 1, Max - 1)
    end,

    %% Combine with existing exceptions and remove exception for
    %% value that we just learned, if it exists.
    Exceptions = (Exceptions0 ++ NewExceptions) -- [Count],

    orddict:store(Actor, #state{counter=Max,
                                exceptions=Exceptions}, Vector).

%% Internal functions.

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

learn_test() ->
    A0 = new(),
    B0 = new(),

    %% Generate actor a.
    {ok, VersionA1, A1} = generate(a, A0),
    ?assertMatch({a, 1}, VersionA1),

    %% Learn.
    A4 = learn({a, 4}, A1),
    ?assertEqual(orddict:from_list([{a, {state, 4, [2, 3]}}]), A4),

    A24 = learn({a, 2}, A4),
    ?assertEqual(orddict:from_list([{a, {state, 4, [3]}}]), A24),

    A245 = learn({a, 5}, A24),
    ?assertEqual(orddict:from_list([{a, {state, 5, [3]}}]), A245),

    B4 = learn({b, 4}, B0),
    ?assertEqual(orddict:from_list([{b, {state, 4, [1, 2, 3]}}]), B4).

merge_with_exceptions_test() ->
    A0 = new(),
    B0 = new(),

    %% Generate actor a.
    {ok, VersionA1, A1} = generate(a, A0),
    ?assertMatch({a, 1}, VersionA1),

    %% Learn.
    A4 = learn({a, 4}, A1),
    ?assertEqual(orddict:from_list([{a, {state, 4, [2, 3]}}]), A4),

    A5 = learn({a, 5}, new()),

    B4 = learn({b, 4}, B0),
    ?assertEqual(orddict:from_list([{b, {state, 4, [1, 2, 3]}}]), B4),

    A4B4 = merge(A4, B4),
    ?assertEqual(orddict:from_list([{a, {state, 4, [2, 3]}},
                                    {b, {state, 4, [1, 2, 3]}}]), A4B4),

    A5B4 = merge(A5, A4B4),
    ?assertEqual(orddict:from_list([{a, {state, 5, [2, 3]}},
                                    {b, {state, 4, [1, 2, 3]}}]), A5B4).

merge_test() ->
    A0 = new(),

    %% Generate actor a.
    {ok, VersionA1, A1} = generate(a, A0),
    ?assertMatch({a, 1}, VersionA1),

    %% Generate actor b.
    {ok, VersionB1, B1} = generate(b, A0),
    ?assertMatch({b, 1}, VersionB1),

    A1B1 = merge(A1, B1),
    ?assertEqual(orddict:from_list([{a, {state, 1, []}}, {b, {state, 1, []}}]), A1B1).

dominates_with_exceptions_test() ->
    A0 = new(),

    %% Generate actor a.
    {ok, VersionA1, A1} = generate(a, A0),
    ?assertMatch({a, 1}, VersionA1),
    ?assertMatch(true, dominates(A0, A1)),

    %% Learn.
    A4 = learn({a, 4}, A1),
    ?assertEqual(orddict:from_list([{a, {state, 4, [2, 3]}}]), A4),
    ?assertMatch(true, dominates(A4, A4)),
    ?assertMatch(true, dominates(A1, A4)),
    ?assertMatch(true, dominates(A0, A4)),
    ?assertMatch(false, dominates(A4, A0)),

    A42 = learn({a, 2}, A4),
    ?assertEqual(orddict:from_list([{a, {state, 4, [3]}}]), A42),
    A43 = learn({a, 3}, A4),
    ?assertEqual(orddict:from_list([{a, {state, 4, [2]}}]), A43),

    ?assertMatch(true, dominates(A4, A42)),
    ?assertMatch(false, dominates(A42, A4)),

    ?assertMatch(true, dominates(A4, A43)),
    ?assertMatch(false, dominates(A43, A4)),

    ?assertMatch(false, dominates(A43, A42)),
    ?assertMatch(false, dominates(A42, A43)).

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
