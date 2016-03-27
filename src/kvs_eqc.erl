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

%% State record.
-record(state, {status, objects, replicas}).

-include("kvs.hrl").

%% Generators.

key() ->
    elements([x, y, z]).

value() ->
    int().

%% Initial state.

initial_state() ->
    #state{status=init, replicas=[]}.

%% Launch multiple replicas.

provision(Actor) ->
    {ok, Pid} = kvs:start_link(Actor),
    Pid.

provision_args(_S) ->
    [elements([a, b, c])].

provision_pre(#state{replicas=Replicas, status=init}, [Actor]) ->
    not lists:member(Actor, [A || {A, _, _} <- Replicas]);
provision_pre(#state{status=running}, [_Actor]) ->
    false.

provision_next(#state{replicas=Replicas0}=S, Pid, [Actor]) ->
    Objects = dict:new(),
    Replicas = Replicas0 ++ [{Actor, Pid, Objects}],
    Provisioned = [A || {A, _, _} <- Replicas],
    Status = case Provisioned of
        [a, b, c] ->
            running;
        _ ->
            init
    end,
    S#state{status=Status, replicas=Replicas}.

%% Get operation.

get(Pid, Key) ->
    kvs:get(Pid, Key).

get_args(#state{replicas=Replicas}) ->
    Pids = [P || {_, P, _} <- Replicas],
    ?LET({Pid, Key}, {elements(Pids), key()},
         begin
            [Pid, Key]
         end).

get_pre(#state{status=running}) ->
    true;
get_pre(#state{status=init}) ->
    false.

get_post(#state{replicas=Replicas0}, [Pid, Key], {ok, not_found}) ->
    {_Actor, Pid, Objects0} = lists:keyfind(Pid, 2, Replicas0),
    case dict:is_key(Key, Objects0) of
        false ->
            true;
        true ->
            false
    end;
get_post(#state{replicas=Replicas0}, [Pid, Key], {ok, Value}) ->
    {_Actor, Pid, Objects0} = lists:keyfind(Pid, 2, Replicas0),
    case dict:find(Key, Objects0) of
        {ok, V} ->
            V =:= Value;
        _ ->
            false
    end.

%% Put operation.

put(Pid, Key, Value) ->
    {ok, ReturnValue} = kvs:put(Pid, Key, Value),
    ReturnValue.

put_args(#state{replicas=Replicas}) ->
    Pids = [P || {_, P, _} <- Replicas],
    ?LET({Pid, Key, Value}, {elements(Pids), key(), value()},
         begin
            [Pid, Key, Value]
         end).

put_pre(#state{status=init}) ->
    false;
put_pre(#state{status=running}) ->
    true.

put_next(#state{replicas=Replicas0}=S, ReturnValue, [Pid, Key, _Value]) ->
    {Actor, Pid, Objects0} = lists:keyfind(Pid, 2, Replicas0),
    Objects = dict:store(Key, ReturnValue, Objects0),
    Replicas = lists:keyreplace(Pid, 2, Replicas0, {Actor, Pid, Objects}),
    S#state{replicas=Replicas}.

%% Synchronize.

synchronize(Pid, ToPid) ->
    kvs:synchronize(Pid, ToPid).

synchronize_args(#state{replicas=Replicas}) ->
    Pids = [P || {_, P, _} <- Replicas],
    ?LET(Pid, elements(Pids),
         ?LET(ToPid, elements(Pids -- [Pid]),
              begin
                  [Pid, ToPid]
              end)).

synchronize_pre(#state{status=running}) ->
    true;
synchronize_pre(#state{status=init}) ->
    false.

synchronize_next(#state{replicas=Replicas0}=S, {ok, Synced}, [Pid, _ToPid]) ->
    {Actor, Pid, Objects0} = lists:keyfind(Pid, 2, Replicas0),
    Objects = lists:foldl(fun({Key, #object{timestamp=Timestamp}=Object}, DictAcc) ->
                                case dict:find(Key, DictAcc) of
                                    {ok, #object{timestamp=CurrentTimestamp}} ->
                                        case CurrentTimestamp > Timestamp of
                                            true ->
                                                DictAcc;
                                            false ->
                                                dict:store(Key, Object, DictAcc)
                                        end;
                                    _ ->
                                        dict:store(Key, Object, DictAcc)
                                end
                          end, Objects0, Synced),
    Replicas = lists:keyreplace(Pid, 2, Replicas0, {Actor, Pid, Objects}),
    S#state{replicas=Replicas};
synchronize_next(#state{replicas=_Replicas0}=S, _Res, [_Pid, _ToPid]) ->
    S.

%% Properties.

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

prop_parallel() ->
    eqc:quickcheck(?SETUP(fun() ->
                                 setup(),
                                 fun teardown/0
                          end,
                         ?FORALL(Cmds, parallel_commands(?MODULE),
                                 begin
                                     {H, S, Res} = run_parallel_commands(?MODULE, Cmds),
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
