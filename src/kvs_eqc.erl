-module(kvs_eqc).
-author("Christopher Meiklejohn <christopher.meiklejohn@gmail.com>").

-ifdef(TEST).
-ifdef(EQC).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_statem.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) ->
                io:format(user, Str, Args)
        end, P)).

-record(state, {status, objects, replicas}).

%% Generators.

key() ->
    elements([x, y, z]).

value() ->
    int().

%% Initial state.

initial_state() ->
    Objects = dict:new(),
    #state{status=init, objects=Objects, replicas=[]}.

%% Launch multiple replicas.

provision(Actor) ->
    {ok, Pid} = kvs:start_link(Actor),
    Pid.

provision_args(_S) ->
    [elements([a, b, c])].

provision_pre(#state{replicas=Replicas, status=init}, [Actor]) ->
    not lists:member(Actor, [A || {A, _} <- Replicas]);
provision_pre(#state{status=running}, [_Actor]) ->
    false.

provision_next(#state{replicas=Replicas0}=S, Pid, [Actor]) ->
    Replicas = Replicas0 ++ [{Actor, Pid}],
    Provisioned = [A || {A, _} <- Replicas],
    Status = case Provisioned of
        [a, b, c] ->
            running;
        _ ->
            init
    end,
    S#state{status=Status, replicas=Replicas}.

%% Put operation.

get(Pid, Key) ->
    kvs:get(Pid, Key).

get_args(#state{replicas=Replicas, objects=Objects0}) ->
    Keys = dict:fetch_keys(Objects0),
    Pids = [P || {_, P} <- Replicas],
    ?LET({Pid, Key}, {elements(Pids), elements(Keys)},
         begin
            [Pid, Key]
         end).

get_pre(#state{status=running, objects=Objects0}) ->
    Keys = dict:fetch_keys(Objects0),
    length(Keys) > 0;
get_pre(#state{status=init}) ->
    false.

put(Pid, Key, Value) ->
    kvs:put(Pid, Key, Value).

put_args(#state{replicas=Replicas}) ->
    Pids = [P || {_, P} <- Replicas],
    ?LET({Pid, Key, Value}, {elements(Pids), key(), value()},
         begin
            [Pid, Key, Value]
         end).

put_pre(#state{status=init}) ->
    false;
put_pre(#state{status=running}) ->
    true.

put_next(#state{objects=Objects0}=S, _Res, [_Pid, Key, Value]) ->
    Objects = dict:store(Key, Value, Objects0),
    S#state{objects=Objects}.

prop_sequential() ->
    eqc:quickcheck(?SETUP(fun() ->
                                 setup(),
                                 fun teardown/0
                          end,
                         ?FORALL(Cmds, commands(?MODULE),
                                 begin
                                     {H, S, Res} = run_commands(?MODULE, Cmds),
                                     pretty_commands(?MODULE, Cmds, {H, S, Res},
                                        aggregate(command_names(Cmds), Res == ok))
                                 end))).

setup() ->
    {ok, _Apps} = application:ensure_all_started(lager),
    ok.

teardown() ->
    ok.

-endif.
-endif.
