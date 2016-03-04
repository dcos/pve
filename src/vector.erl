%% @doc Version vector interface.

-module(vector).
-author("Christopher Meiklejohn <christopher.meiklejohn@gmail.com>").

%% Types from vector types.
-type vector() :: pv:vector().
-type update() :: pv:update().
-type actor()  :: pv:actor().

%% @doc Generate a new predecessor vector.
-callback new() -> vector().

%% @doc Increment a predecessor vector and return the object's version.
-callback increment(actor(), vector()) -> {ok, update(), vector()}.

%% @doc Determine if `Vector1' strictly dominates `Vector2'.
-callback strict_dominates(vector(), vector()) -> boolean().

%% @doc Determine if `Vector1' dominates `Vector2'.
-callback dominates(vector(), vector()) -> boolean().

%% @doc Merge two vectors.
-callback merge(vector(), vector()) -> vector().

%% @doc Accumulate knowledge for an update into vector.
-callback learn(update(), vector()) -> vector().
