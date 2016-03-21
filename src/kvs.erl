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

%% Use the vector!
-define(VECTOR, pve).

%% API
-export([synchronize/2,
         put/3,
         get/2]).

%% State record.
-record(state, {actor,
                knowledge :: ?VECTOR:vector(),
                objects}).

%% Object record.
-record(object, {version,
                 predecessors :: ?VECTOR:vector(),
                 payload}).

%% Types.
-type actor() :: atom().
-type key()   :: term().
-type value() :: term().

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Start and link to calling process.
-spec start_link(actor())-> {ok, pid()} | ignore | {error, term()}.
start_link(Actor) ->
    gen_server:start_link(?MODULE, [Actor], []).

%% @doc Get a value from the KVS.
-spec get(pid(), key()) -> value().
get(Pid, Key) ->
    gen_server:call(Pid, {get, Key}, infinity).

%% @doc Store a value in the KVS.
-spec put(pid(), key(), value()) -> ok.
put(Pid, Key, Value) ->
    gen_server:call(Pid, {put, Key, Value}, infinity).

%% @doc Synchronize with another KVS.
-spec synchronize(pid(), pid()) -> ok.
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
        {ok, #object{payload=Payload}} ->
            {ok, Payload};
        error ->
             {ok, not_found}
    end,
    {reply, Result, State};
handle_call({put, Key, Value}, _From, #state{actor=Actor,
                                             knowledge=Knowledge0,
                                             objects=Objects0}=State) ->
    %% Generate the version.
    {ok, Version, Knowledge} = ?VECTOR:generate(Actor, Knowledge0),

    %% Generate object payload and store.
    Object = #object{version=Version, payload=Value},

    %% Store updated version of object.
    Objects = dict:store(Key, Object, Objects0),

    {reply, ok, State#state{objects=Objects, knowledge=Knowledge}};
handle_call({synchronize, ToPid}, _From, #state{knowledge=Knowledge0}=State0) ->
    %% Synchronization is a three-step process.
    Self = self(),

    %% 1. Send the other node your knowledge set.
    ToPid ! {synchronize, Self, Knowledge0},

    %% 2. Wait to receive and process objects from other replica.
    State = receive_and_process_objects(State0),

    {reply, ok, State};
handle_call(Msg, _From, State) ->
    lager:warning("Unhandled messages: ~p", [Msg]),
    {reply, ok, State}.

%% @private
-spec handle_cast(term(), #state{}) -> {noreply, #state{}}.
handle_cast(Msg, State) ->
    lager:warning("Unhandled messages: ~p", [Msg]),
    {noreply, State}.

%% @private
-spec handle_info(term(), #state{}) -> {noreply, #state{}}.
handle_info({synchronize, FromPid, Knowledge},
            #state{objects=Objects}=State) ->
    lager:info("Received request for objects from ~p.", [FromPid]),

    %% Send objects that are not dominated by the incoming vector.
    dict:fold(fun(Key, #object{version=Version}=Value, Acc) ->
                    case ?VECTOR:dominates(Version, Knowledge) of
                        true ->
                            lager:info("Object dominated; skipping."),
                            Acc;
                        false ->
                            lager:info("Object not dominated; sending!"),
                            FromPid ! {object, Key, Value},
                            Acc + 1
                    end
              end, 0, Objects),

    %% Reply when sending of objects is complete.
    FromPid ! done,

    lager:info("Done iterating objects."),

    {noreply, State};
handle_info(Msg, State) ->
    lager:warning("Unhandled messages: ~p", [Msg]),
    {noreply, State}.

%% @private
-spec terminate(term(), #state{}) -> term().
terminate(_Reason, _State) ->
    ok.

%% @private
-spec code_change(term() | {down, term()}, #state{}, term()) -> {ok, #state{}}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @doc This is a continuation of the syncrhonization process started in
%%      the handle_info synchronization callback.
%%
receive_and_process_objects(#state{knowledge=Knowledge,
                                   objects=Objects0}=State) ->
    receive
        {object, Key, #object{version=TheirVersion,
                              predecessors=TheirPredecessors}=Value} ->
            lager:info("Received object; ~p", [Key]),

            case dict:find(Key, Objects0) of

                %% We have it.
                {ok, #object{version=OurVersion,
                             predecessors=OurPredecessors}} ->

                    %% If the incoming object is dominated, ignore it and stop.
                    case ?VECTOR:dominates(TheirVersion, OurPredecessors) of

                        true ->
                            %% Ignore object; already dominated.
                            receive_and_process_objects(State);

                        false ->

                            %% If this dominates, then replace.
                            case ?VECTOR:dominates(OurVersion,
                                                   TheirPredecessors) of
                                true ->

                                    %% Replace object.
                                    Objects = dict:store(Key,
                                                         Value,
                                                         Objects0),

                                    %% Insert incoming version into
                                    %% replica knowledge.
                                    Knowledge1 = ?VECTOR:learn(TheirVersion,
                                                               Knowledge),

                                    %% Merge incoming predecessors into
                                    %% replica knowledge.
                                    Knowledge2 = ?VECTOR:merge(TheirPredecessors,
                                                               Knowledge1),

                                    receive_and_process_objects(State#state{knowledge=Knowledge2,
                                                                            objects=Objects});

                                %% If not, throw conflict, which is fine
                                %% for this simple test KVS.
                                false ->
                                    exit(conflict)

                            end
                    end;

                %% We don't have the object locally, so store it with
                %% predecessors and advance the replica's knowledge
                %% vector.
                error ->

                    %% Replace object in store.
                    Objects = dict:store(Key, Value, Objects0),

                    %% Insert incoming version into replica knowledge.
                    Knowledge1 = ?VECTOR:learn(TheirVersion, Knowledge),

                    %% Merge incoming predecessors into replica knowledge.
                    Knowledge2 = ?VECTOR:merge(TheirPredecessors,
                                               Knowledge1),

                    receive_and_process_objects(State#state{knowledge=Knowledge2,
                                                            objects=Objects})

            end;
        done ->
            ok
    end.
