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
-export([put/3,
         get/2]).

%% State record.
-record(state, {actor,
                knowledge,
                objects}).

%% Object record.
-record(object, {version,
                 payload}).

%% Types.
-type actor() :: atom().
-type key()   :: term().
-type value() :: term().

%% Use the vector!
-define(VECTOR, pv).

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
    %% Increment the version.
    {ok, Version, Knowledge} = ?VECTOR:increment(Actor, Knowledge0),

    %% Generate object payload and store.
    Object = #object{version=Version, payload=Value},

    %% Store updated version of object.
    Objects = dict:store(Key, Object, Objects0),

    {reply, ok, State#state{objects=Objects, knowledge=Knowledge}};
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
