%% @doc Predecessor vector interface.

-module(vector).
-author("Christopher Meiklejohn <christopher.meiklejohn@gmail.com>").

%% Types from vector types.
-type vector()    :: pv:vector() | pve:vector().
-type version()   :: pv:version() | pve:version().
-type actor()     :: pv:actor() | pve:actor().

%% @doc Generate a new predecessor vector.
-callback new() -> vector().

%% @doc Generate a new version, return the version and the updated
%%      predecessor vector.
-callback generate(actor(), vector()) -> {ok, version(), vector()}.

%% @doc Determine if a vector or an version is dominated by another
%%      vector.
-callback strictly_dominates(version() | vector(), vector()) -> boolean().

%% @doc Determine if a vector or an version is dominated by another
%%      vector.
-callback dominates(version() | vector(), vector()) -> boolean().

%% @doc Merge two vectors.
-callback merge(vector(), vector()) -> vector().

%% @doc Accumulate knowledge for an version into vector.
-callback learn(version(), vector()) -> vector().
