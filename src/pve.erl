%% @doc Predecessor vector with negative exceptions.

-module(pve).
-author("Christopher Meiklejohn <christopher.meiklejohn@gmail.com>").

-export([new/0,
         increment/2,
         dominates/2,
         merge/2,
         learn/2,
         strict_dominates/2]).

-behaviour(vector).

%% Record specifications.
-record(state, {counter, exceptions = []}).

%% Type specifications.
-type actor()     :: atom().
-type count()     :: non_neg_integer().
-type update()    :: {actor(), count()}.
-type exception() :: non_neg_integer().
-type vector()    :: orddict:orddict(actor(), state()).
-type state()     :: #state{counter :: count(), exceptions :: [exception()]}.

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
strict_dominates(_Vector1, _Vector2) ->
    {error, not_implemented}.

%% @doc Determine if `Vector1' dominates `Vector2'.
-spec dominates(vector(), vector()) -> boolean().
dominates(_Vector1, _Vector2) ->
    {error, not_implemented}.

%% @doc Merge two vectors.
-spec merge(vector(), vector()) -> vector().
merge(_Vector1, _Vector2) ->
    {error, not_implemented}.

%% @doc Accumulate knowledge for an update into vector.
-spec learn(update(), vector()) -> vector().
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

    %% Increment actor a.
    {ok, UpdateA1, A1} = increment(a, A0),
    ?assertMatch({a, 1}, UpdateA1),

    %% Learn.
    A4 = learn({a, 4}, A1),
    ?assertEqual(orddict:from_list([{a, {state, 4, [2, 3]}}]), A4),

    A24 = learn({a, 2}, A4),
    ?assertEqual(orddict:from_list([{a, {state, 4, [3]}}]), A24),

    A245 = learn({a, 5}, A24),
    ?assertEqual(orddict:from_list([{a, {state, 5, [3]}}]), A245),

    B4 = learn({b, 4}, B0),
    ?assertEqual(orddict:from_list([{b, {state, 4, [1, 2, 3]}}]), B4).

-endif.
