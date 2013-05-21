%% -------------------------------------------------------------------
%%
%% Copyright (c) 2013 Basho Technologies, Inc.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------
-module(sidejob).
-export([new_resource/3, new_resource/4, call/2, call/3, cast/2]).

-ifndef(TEST).
-define(TEST, 1).
-endif.

-ifdef(TEST).
-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_statem.hrl").
-include_lib("eunit/include/eunit.hrl").
-compile(export_all).
-behaviour(eqc_statem).
-export([command/1, initial_state/0, next_state/3,
         precondition/2, postcondition/3]).

-endif.

%%%===================================================================
%%% API
%%%===================================================================

%% @doc
%% Create a new sidejob resource that uses the provided worker module,
%% enforces the requested usage limit, and is managed by the specified
%% number of worker processes.
%%
%% This call will generate and load a new module, via {@link sidejob_config},
%% that provides information about the new resource. It will also start up the
%% supervision hierarchy that manages this resource: ensuring that the workers
%% and stats aggregation server for this resource remain running.
new_resource(Name, Mod, Limit, Workers) ->
    ETS = sidejob_resource_sup:ets(Name),
    StatsETS = sidejob_resource_sup:stats_ets(Name),
    WorkerNames = sidejob_worker:workers(Name, Workers),
    StatsName = sidejob_resource_stats:reg_name(Name),
    WorkerLimit = Limit div Workers,
    sidejob_config:load_config(Name, [{width, Workers},
                                      {limit, Limit},
                                      {worker_limit, WorkerLimit},
                                      {ets, ETS},
                                      {stats_ets, StatsETS},
                                      {workers, list_to_tuple(WorkerNames)},
                                      {stats, StatsName}]),
    sidejob_sup:add_resource(Name, Mod).

%% @doc
%% Same as {@link new_resource/4} except that the number of workers defaults
%% to the number of scheduler threads.
new_resource(Name, Mod, Limit) ->
    Workers = erlang:system_info(schedulers),
    new_resource(Name, Mod, Limit, Workers).


%% @doc
%% Same as {@link call/3} with a default timeout of 5 seconds.
call(Name, Msg) ->
    call(Name, Msg, 5000).

%% @doc
%% Perform a synchronous call to the specified resource, failing if the
%% resource has reached its usage limit.
call(Name, Msg, Timeout) ->
    case available(Name) of
        none ->
            overload;
        Worker ->
            gen_server:call(Worker, Msg, Timeout)
    end.

%% @doc
%% Perform an asynchronous cast to the specified resource, failing if the
%% resource has reached its usage limit.
cast(Name, Msg) ->
    ?debugFmt("Cast ~p, ~p", [Name,Msg]),
    case available(Name) of
        none ->
            ?debugMsg("Resource unavailable, overload"),
            overload;
        Worker ->
            ?debugFmt("Resource ~p available, sending to ~p", [Name, Worker]),
            gen_server:cast(Worker, Msg)
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% Find an available worker or return none if all workers at limit
available(Name) ->
    ETS = Name:ets(),
    Width = Name:width(),
    Limit = Name:worker_limit(),
    Scheduler = erlang:system_info(scheduler_id),
    Worker = Scheduler rem Width,
    ?debugFmt("Available on scheduler ~p, worker ~p", [Scheduler, Worker]),
    ?debugFmt("ETS table for ~p : ~n~p", [Name, ets:tab2list(ETS)]),
    case is_available(ETS, Limit, Worker) of
        true ->
            ?debugFmt("Resource is available ~p", [[ETS, Limit, Worker]]),
            worker_reg_name(Name, Worker);
        false ->
            %%?debugFmt("Resource is NOT available ~p", [[ETS, Limit, Worker]]),
            available(Name, ETS, Width, Limit, Worker+1, Worker)
    end.

available(Name, _ETS, _Width, _Limit, End, End) ->
    ?debugFmt("Available reached end ~p", [[Name, End, End]]),
    ets:update_counter(Name:stats_ets(), rejected, 1),
    none;
available(Name, ETS, Width, Limit, X, End) ->
    Worker = X rem Width,
    %%?debugFmt("Trying another ~p ~p ~p", [X, Worker, End]),
    case is_available(ETS, Limit, Worker) of
        false ->
            %%?debugFmt("Resource is NOT available ~p", [[ETS, Limit, Worker]]),
            available(Name, ETS, Width, Limit, Worker+1, End);
        true ->
            ?debugFmt("Resource is available ~p", [[ETS, Limit, Worker]]),
            worker_reg_name(Name, Worker)
    end.

is_available(ETS, Limit, Worker) ->
    ?debugFmt("is_available ~p", [[ETS, Limit, Worker]]),
    case ets:lookup_element(ETS, {full, Worker}, 2) of
        1 ->
            ?debugMsg("Is full, NO"),
            false;
        0 ->
            ?debugMsg("It's not full, incrementing counter"),
            Value = ets:update_counter(ETS, Worker, 1),
            case Value >= Limit of
                true ->
                    ets:insert(ETS, {{full, Worker}, 1}),
                    ?debugMsg("Marking full"),
                    false;
                false ->
                    ?debugMsg("Return true"),
                    true
            end,
            true
    end.

worker_reg_name(Name, Id) ->
    element(Id+1, Name:workers()).

-ifdef(TEST).

%% Model for EQC:
%% Have N workers, never more than num schedulers.
%% They hold back calls until they get special messages to keep going.
%% Events:
%%   A request, which should fail with overload if resources exhausted
%%   A request which ends up being handled by a worker.
%%   A request when resources exist but all workers are blocked. It is sent and global counter incremented.
%%   A request completion, which sends a message to a blocked worker to complete the request, decr global counter
%%

-record(test_state, {
        next_request=1,
        n = 0, % Number of requets in flight (some may have returned overload due to fuzziness)
        max = 0,
        good_threshold, % Below this ALL requests should be taken
        overload_threshold, % Above this ALL requests should return overload
        blocking
        }).

-record(spy_state, {blocked=sets:new(), pending}).

-define(NUM_TESTS, 100).
-define(SJT_RESOURCE, sjt_resource).
-define(SJT_MSG, sjt_msg).
-define(SJT_WORKER_SPY, sjt_worker_spy).

sjt_eqc_test() ->
    ?assert(eqc:quickcheck(eqc:numtests(?NUM_TESTS, ?MODULE:sjt_prop()))).

sjt_prop() ->
    ?FORALL(Cmds, commands(?MODULE),
            ?TRAPEXIT(
            begin
                ?debugMsg("Starting sidejob application"),
                application:start(sidejob),
                sjt_start_worker_spy(),
                try 
                    ?debugMsg("===================== Starting a run ========================="),
                    {H, S, R} = run_commands(?MODULE, Cmds),
                    ?debugMsg("===================== Ending a run ========================="),
                    eqc_gen:with_parameter(show_states, true,
                                           pretty_commands(?MODULE, Cmds, {H, S, R}, R == ok))
                after
                    sjt_stop_worker_spy(),
                    ?debugMsg("Stopping sidejob application"),
                    application:stop(sidejob)
                end
            end)).
initial_state() ->
    #test_state{}.

positive_nat() ->
    ?LET(N, nat(), N+1).

command(#test_state{max=0}) ->
    NSched = erlang:system_info(schedulers),
    {call, ?MODULE, sjt_init, [choose(1, NSched), positive_nat()]};
command(#test_state{n=N, max=Max}) when N >= Max ->
    {call, ?MODULE, sjt_rejected_request, [oneof([call, cast])]}; 
command(#test_state{n=N, blocking=Blocking}) ->
    oneof([{call, ?MODULE, sjt_start_request, [oneof([cast])]}]
          ++ 
          [{call, ?MODULE, sjt_end_request, [Blocking]} || N > 0]).

next_state(S, _V, {call, _, sjt_init, [NWorkers, WorkerLimit]}) ->
   S#test_state{max=NWorkers*WorkerLimit}; 
next_state(S = #test_state{n=N}, V, {call, _, sjt_start_request, _}) ->
    S#test_state{n=N+1, blocking = V};
next_state(S = #test_state{n=1}, _V, {call, _, sjt_end_request, []}) ->
    S#test_state{n=0, blocking=undefined};
next_state(S = #test_state{n=N}, V, {call, _, sjt_end_request, _}) ->
    S#test_state{n=N-1, blocking=V};
next_state(S, _V, {call, _, sjt_rejected_request, _}) ->
    S.

precondition(#test_state{max=0}, {call, _, sjt_init, _}) ->
    true;
precondition(#test_state{n=N, max=Max}, {call, _, sjt_start_request, _}) ->
    N < Max;
precondition(#test_state{n=N, max=Max}, {call, _, sjt_rejected_request, _}) ->
    Max > 0 andalso N >= Max;
precondition(#test_state{n=N}, {call, _, sjt_end_request, _}) ->
    N > 0.

% We fail only if functions crash
postcondition(_S, _Cmd, _R) ->
    true.

sjt_worker_spy_loop(S = #spy_state{blocked=Blocked, pending=Pending}) ->
    NewS = receive
        die ->
            exit(normal);
        {worker_blocking, Pid} ->
            NewBlocked = sets:add_element(Pid, Blocked),
            case Pending of
                undefined ->
                    ok;
                {ReqRef, ReqPid} ->
                    ReqPid ! {ReqRef, NewBlocked}
            end,
            S#spy_state{blocked=NewBlocked};
        {worker_unblocking, Pid} ->
            S#spy_state{blocked=sets:del_element(Pid, Blocked)};
        {get_blocking_set, Pid, Ref} ->
            case sets:size(Blocked) of
                0 ->
                    Pending = {Ref, Pid},
                    S#spy_state{pending=Pending};
                _ ->
                    Pid ! {Ref, Blocked},
                    S
            end
    end,
    sjt_worker_spy_loop(NewS).

sjt_start_worker_spy() ->
    ?debugFmt("Starting worker spy process", []),
    Pid = spawn_link(?MODULE, sjt_worker_spy_loop, [#spy_state{}]),
    true = register(?SJT_WORKER_SPY, Pid),
    ?debugFmt("Worker spy process on ~p~n", [Pid]),
    ok.

sjt_stop_worker_spy() ->
    ?debugFmt("Stopping worker spy process", []),
    case whereis(?SJT_WORKER_SPY) of
        undefined ->
            ok;
        Pid ->
            MRef = monitor(process, ?SJT_WORKER_SPY),
            Pid ! die,
            ?debugFmt("Set up monitor for worker spy, wait for death", []),
            receive
                {'DOWN', MRef, process, _, _} -> ok
            after
                5000 ->
                    throw(stop_worker_spy_timed_out)
            end
    end.


sjt_init(NWorkers, WorkerLimit) ->
    ?debugFmt("Setting up resource ~p, ~p~n", [NWorkers, WorkerLimit]),
    sidejob:new_resource(?SJT_RESOURCE, sjt_eqc_worker, NWorkers*WorkerLimit, NWorkers),
    ?debugMsg("Resource is up and should be ready").

sjt_rejected_request(call) ->
    ?debugFmt("Issuing call that will be rejected", []),
    overload = sidejob:call(?SJT_RESOURCE, sjt_next_msg(), 0);
sjt_rejected_request(cast) ->
    ?debugFmt("Issuing cast that will be rejected ~n", []),
    overload = sidejob:cast(?SJT_RESOURCE, sjt_next_msg()). 

sjt_request(ReqId) -> {sjt_req, ReqId}.

%% Start a request that should be accepted.
%% A cast will not return overload,
sjt_start_request(cast, ReqId) ->
    Req = sjt_request(ReqId),
    ?debugFmt("Will issue cast ~p that will be accepted~n", [ReqId]),
    ok = sidejob:cast(?SJT_RESOURCE, Req),
    ?debugMsg("Cast was issued, waiting until queued"),
    sjt_worker_spy:wait_for_request(Req),
    ok;
%% A call will either block a worker that starts processing it or may
%% end up in the message queue for one of the workers. Notice that even if
%% some workers are not blocking on requests, sidejob does not guarantee that
%% one of those will get the next request.
sjt_start_request(call, ReqId) ->
    Req = sjt_request(ReqId),
    ?debugFmt("Will issue call that will be accepted~n", []),
    spawn_link(fun() -> ok = sidejob:call(?SJT_RESOURCE, Req) end),
    ?debugMsg("Call was issued in separate process, now wait until queued"),
    sjt_eqc_worker_spy:wait_for_request(Req),
    ?debugMsg("Call was queued up, next!"),
    ok.

% Start a request that may fail from overload or may be queued
sjt_start_fuzzy_request(cast, ReqId) ->
    Req = to_req(ReqId),
    case sidejob:cast(?SJT_RESOURCE, Req) of
        overload -> ok;
        ok -> ok
    end,
    ok;
sjt_start_fuzzy_request(call, ReqId) ->
    Req = to_req(ReqId),
    spawn_link(fun() ->
                      case sidejob:call(?SJT_RESOURCE, Req) of
                          overload ->
                              sjt_eqc_worker_spy:count_fuzzy_overload();
                          ok ->
                              ok
                      end 
               end),
    ok.

sjt_end_request(Blocking) ->
    BlockingList = sets:to_list(Blocking),
    ?debugFmt("Will release one of the blocked requests ~p~n", [BlockingList]),
    One = hd(BlockingList),
    Ref = make_ref(),
    One ! {unblock, self(), Ref},
    receive
        {unblock_received, Ref} ->
            ok
    after
        5000 ->
            ?debugMsg("Request release timed out!"),
            throw(sjt_unblock_reply_timed_out)
    end.

sjt_get_blocking_set() ->
    ?debugMsg("Requesting blocking set from worker spy process~n"),
    % Request blocking set from worker spy, if timeout test fails
    Ref = make_ref(),
    ?SJT_WORKER_SPY ! {get_blocking_set, self(), Ref},
    receive
        {Ref, BlockingSet} ->
            ?debugFmt("Received blocking set ~p~n", [sets:to_list(BlockingSet)]),
            BlockingSet
    after
        5000 ->
            ?debugMsg("Timed out waiting for blocking set~n"),
            throw(get_blocking_set_timed_out)
    end.

-endif.
