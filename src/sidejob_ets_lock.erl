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

%% @doc
%% This module implements the sidejob_worker logic used by all worker
%% processes created to manage a sidejob resource. This code emulates
%% the gen_server API, wrapping a provided user-specified module which
%% implements the gen_server behavior.
%%
%% The primary purpose of this module is updating the usage information
%% published in a given resource's ETS table, such that capacity limiting
%% operates correctly. The sidejob_worker also cooperates with a given
%% {@link sidejob_resource_stats} server to maintain statistics about a
%% given resource.
%%
%% By default, a sidejob_worker calculates resource usage based on message
%% queue size. However, the user-specified module can also choose to
%% implement the `current_usage/1' and `rate/1' callbacks to change how
%% usage is calculated. An example is the {@link sidejob_supervisor} module
%% which reports usage as: queue size + num_children.

-module(sidejob_ets_lock).
-behaviour(gen_server).

%% API
-export([start_link/2, update_usage/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {
          ets             :: term(),
          name
         }).

%%%===================================================================
%%% API
%%%===================================================================
start_link(Name, ETS) ->
    gen_server:start_link({local, Name}, ?MODULE, [Name,ETS], []).

update_usage(ETS, Usage) ->
    gen_server:call(ets_name(ETS), {update_usage, Usage}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================
init([Name, ETS]) ->
    State = #state{name=Name, ets=ETS},
    {ok, State}.

handle_call({available, Limit}, _From, State) ->
    #state{ets=ETS, name=Name} = State,
    Res={_WasFull, _IsFull, _Value} = case ets:lookup_element(ETS, full, 2) of
                                          1 ->
                                              {true, true, unkown};
                                          0 ->
                                              %% Usage = ets:lookup_element(ETS, usage, 2),
                                              %% pulse:format("usage ~p~n", [Usage]),
                                              %% Val = Usage +1,
                                              %% ets:insert(ETS, {usage, Val}),
                                              %% Usage2 = ets:lookup_element(ETS, usage, 2),
                                              %% pulse:format("usage2 ~p~n", [Usage2]),
                                              Value = ets:update_counter(ETS, usage, 1),
                                              if Value >= Limit ->
                                                      pulse:format("~p ~p updating to full!~n", [Name, self()]),
                                                      ets:insert(ETS, {full, 1}),
                                                      pulse:format("Full value is ~p~n", [ets:lookup_element(ETS, full, 2)]);
                                                 true ->
                                                      ok
                                              end,
                                              {false, Value >= Limit, Value}
                                      end,
    {reply, Res, State};
handle_call({update_usage, Usage}, _From, State) ->
    #state{ets=ETS, name=_Name} = State,
    ets:insert(ETS, Usage),
    {reply, ok, State}.

handle_cast( _, State) ->
    {noreply, State}.

handle_info(_, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
ets_name(ETS) ->
    EtsLockNameBin = <<"etz_", (atom_to_binary(ETS, latin1))/binary>>,
    binary_to_atom(EtsLockNameBin, latin1).
