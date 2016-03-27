-module(kvs).
-author("Christopher Meiklejohn <christopher.meiklejohn@gmail.com>").

%% API
-export([start_link/1]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

%% API
-export([synchronize/2,
         put/3,
         get/2]).

-include("kvs.hrl").

%% State record.
-record(state, {actor,
                knowledge :: ?VECTOR:vector(),
                objects}).

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Start and link to calling process.
-spec start_link(actor())-> {ok, pid()} | ignore | {error, term()}.
start_link(Actor) ->
    gen_server:start_link(?MODULE, [Actor], []).

%% @doc Get a value from the KVS.
-spec get(pid(), key()) -> {ok, value()}.
get(Pid, Key) ->
    gen_server:call(Pid, {get, Key}, infinity).

%% @doc Store a value in the KVS.
-spec put(pid(), key(), value()) -> {ok, value()}.
put(Pid, Key, Value) ->
    gen_server:call(Pid, {put, Key, Value}, infinity).

%% @doc Synchronize with another KVS.
-spec synchronize(pid(), pid()) -> {ok, [value()]}.
synchronize(Pid, ToPid) ->
    gen_server:call(Pid, {synchronize, ToPid}, infinity).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%% @private
-spec init([actor()]) -> {ok, #state{}}.
init([Actor]) ->
    %% Start off with no objects.
    Objects = dict:new(),

    %% Start off with zero knowledge.
    Knowledge = ?VECTOR:new(),

    {ok, #state{knowledge=Knowledge, objects=Objects, actor=Actor}}.

%% @private
-spec handle_call(term(), {pid(), term()}, #state{}) ->
    {reply, term(), #state{}}.

%% @private
handle_call({get, Key}, _From, #state{objects=Objects0}=State) ->
    Result = case dict:find(Key, Objects0) of
        {ok, Object} ->
            {ok, Object};
        error ->
            {ok, not_found}
    end,
    {reply, Result, State};
handle_call({put, Key, Value}, _From, #state{actor=Actor,
                                             knowledge=Knowledge0,
                                             objects=Objects0}=State) ->
    %% Generate the version.
    {ok, Version, Knowledge} = ?VECTOR:generate(Actor, Knowledge0),

    %% If the object already exists, grab it's predecessors vector and
    %% update with the new version we just created; else use a new
    %% vector.
    Predecessors0 = case dict:find(Key, Objects0) of
        {ok, #object{predecessors=[]}} ->
            %% If the predecessors are empty, causally precedes the
            %% previous update, so the predecessor vector can remain
            %% empty (or at bottom.)
            [];
        {ok, #object{predecessors=P}} ->
            %% If not, insert the new version into the predecessor
            %% vector, without any execption (we do that below.)
            P;
        error ->
            ?VECTOR:new()
    end,
    %% @todo: The paper says to insert this version with no
    %%        execeptions, but I'm not sure that's actually correct.
    %%
    %%        "Implicitly this means that the set of versions that were
    %%        dominated by the previous o.predecessors causally precede
    %%        the new update."
    %%
    %%        But, won't they always causally precede the new update
    %%        regardless of whether or not we insert no execeptions,
    %%        given the predecessor vector will be the previous
    %%        predecessors merged with the new version?  Seems so.
    %%
    Predecessors = ?VECTOR:learn(Version, Predecessors0),

    %% Generate timestamp.
    Timestamp = os:timestamp(),

    %% Generate object payload and store.
    Object = #object{version=Version,
                     timestamp=Timestamp,
                     payload=Value,
                     predecessors=Predecessors},

    %% Store updated version of object.
    Objects = dict:store(Key, Object, Objects0),

    {reply, {ok, Object}, State#state{objects=Objects, knowledge=Knowledge}};
handle_call({synchronize, ToPid}, _From, #state{knowledge=Knowledge0}=State0) ->
    %% Synchronization is a three-step process.
    Self = self(),

    %% 1. Send the other node your knowledge set.
    ToPid ! {synchronize, Self, Knowledge0},

    %% 2. Wait to receive and process objects from other replica.
    {Synced, State} = receive_and_process_objects(State0, []),

    {reply, {ok, Synced}, State};
handle_call(Msg, _From, State) ->
    lager:warning("Unhandled call messages: ~p", [Msg]),
    {reply, ok, State}.

%% @private
-spec handle_cast(term(), #state{}) -> {noreply, #state{}}.
handle_cast(Msg, State) ->
    lager:warning("Unhandled cast messages: ~p", [Msg]),
    {noreply, State}.

%% @private
-spec handle_info(term(), #state{}) -> {noreply, #state{}}.
handle_info({synchronize, FromPid, Knowledge},
            #state{objects=Objects}=State) ->

    %% Send objects that are not dominated by the incoming vector.
    dict:fold(fun(Key, #object{version=Version}=Value, Acc) ->
                    case ?VECTOR:dominates(Version, Knowledge) of
                        true ->
                            Acc;
                        false ->
                            FromPid ! {object, Key, Value},
                            Acc + 1
                    end
              end, 0, Objects),

    %% Reply when sending of objects is complete.
    FromPid ! done,

    {noreply, State};
handle_info(Msg, State) ->
    lager:warning("Unhandled info messages: ~p", [Msg]),
    {noreply, State}.

%% @private
-spec terminate(term(), #state{}) -> term().
terminate(_Reason, _State) ->
    ok.

%% @private
-spec code_change(term() | {down, term()}, #state{}, term()) ->
    {ok, #state{}}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @doc This is a continuation of the syncrhonization process started in
%%      the handle_info synchronization callback.
%%
receive_and_process_objects(State0, Synced) ->
    receive
        {object, Key, Value} ->
            State = process_object(State0, Key, Value),
            receive_and_process_objects(State, Synced ++ [{Key, Value}]);
        done ->
            {Synced, State0}
    end.

%% @private
process_object(#state{objects=Objects0,
                      knowledge=Knowledge}=State,
               Key,
               #object{version=TheirVersion,
                       timestamp=TheirTimestamp,
                       predecessors=TheirPredecessors}=Theirs) ->
    case dict:find(Key, Objects0) of
        %% We have it.
        {ok, #object{version=OurVersion,
                     timestamp=OurTimestamp,
                     predecessors=OurPredecessors}=_Ours} ->

            %% If the incoming object is dominated, ignore it and stop.
            case ?VECTOR:dominates(TheirVersion, OurPredecessors) of
                true ->
                    %% Ignore object; already dominated.
                    State;
                false ->

                    %% If this dominates, then replace.
                    case ?VECTOR:dominates(OurVersion, TheirPredecessors) of
                        true ->
                            %% Replace object.
                            Objects = dict:store(Key, Theirs, Objects0),

                            %% Insert incoming version into replica knowledge.
                            Knowledge1 = ?VECTOR:learn(TheirVersion, Knowledge),

                            %% Merge incoming predecessors into replica knowledge.
                            Knowledge2 = ?VECTOR:merge(TheirPredecessors, Knowledge1),

                            %% Return updated state.
                            State#state{knowledge=Knowledge2, objects=Objects};

                        %% If not, take object with the highest
                        %% timestamp and merge versions, and merge
                        %% predecessor vectors.
                        %%
                        false ->
                            %% Always incorporate their versions into
                            %% our knowledge, as a requirement for the
                            %% model.

                            %% Insert incoming version into replica knowledge.
                            Knowledge1 = ?VECTOR:learn(TheirVersion, Knowledge),

                            %% Merge incoming predecessors into replica knowledge.
                            Knowledge2 = ?VECTOR:merge(TheirPredecessors, Knowledge1),

                            Objects = case OurTimestamp < TheirTimestamp of
                                true ->
                                    %% Replace object with theirs.
                                    dict:store(Key, Theirs, Objects0);
                                false ->
                                    %% Keep our objects.
                                    Objects0
                            end,

                            %% Return updated state.
                            State#state{knowledge=Knowledge2, objects=Objects}
                    end
            end;

        %% We don't have the object locally, so store it with
        %% predecessors and advance the replica's knowledge
        %% vector.
        error ->

            %% Replace object in store.
            Objects = dict:store(Key, Theirs, Objects0),

            %% Insert incoming version into replica knowledge.
            Knowledge1 = ?VECTOR:learn(TheirVersion, Knowledge),

            %% Merge incoming predecessors into replica knowledge.
            Knowledge2 = ?VECTOR:merge(TheirPredecessors,
                                       Knowledge1),

            %% Return udpated state.
            State#state{knowledge=Knowledge2, objects=Objects}

    end.
