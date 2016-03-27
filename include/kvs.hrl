%% Use the vector!
-define(VECTOR, pve).

%% Types.
-type actor()   :: atom().
-type key()     :: term().
-type value()   :: term().
-type counter() :: non_neg_integer().

%% Object record.
-record(object, {version :: {actor(), counter()},
                 timestamp :: os:timestamp(),
                 predecessors :: ?VECTOR:vector(),
                 payload :: term()}).
