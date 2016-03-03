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

-record(state, {objects}).

%% Generators.

key() ->
    elements([x, y, z]).

value() ->
    int().

%% Initial state.

initial_state() ->
    Objects = dict:new(),
    #state{objects=Objects}.

%% Put operation.

get(Key) ->
    kvs:get(Key).

get_args(#state{objects=Objects0}) ->
    Keys = dict:fetch_keys(Objects0),
    ?LET(Key, elements(Keys),
         begin
            [Key]
         end).

get_pre(#state{objects=Objects0}) ->
    Keys = dict:fetch_keys(Objects0),
    length(Keys) > 0.

put(Key, Value) ->
    kvs:put(Key, Value).

put_args(_S) ->
    [key(), value()].

put_next(#state{objects=Objects0}=S, _Res, [Key, Value]) ->
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
    Actor = a,
    {ok, _Apps} = application:ensure_all_started(lager),
    {ok, _Pid} = kvs:start_link(Actor),
    ok.

teardown() ->
    ok.

-endif.
-endif.
