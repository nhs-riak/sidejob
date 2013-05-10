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
%% Utility that converts a given property list into a module that provides
%% constant time access to the various key/value pairs.
%%
%% Example:
%%   load_config(test, [{limit, 1000},
%%                      {num_workers, 4},
%%                      {workers, [{test_1, test_2, test_3, test_4}]}]).
%%
%% creates the module `test' such that:
%%   test:limit().       => 1000
%%   test:num_workers(). => 16
%%   test:workers().     => [{test_1, test_2, test_3, test_4}]}]
%%
-module(sidejob_config).
-export([load_config/2]).

load_config(Resource, Config) ->
    Module = make_module(Resource),
    Exports = [make_export(Key) || {Key, _} <- Config],
    Functions = [make_function(Key, Value) || {Key, Value} <- Config],
    ExportAttr = make_export_attribute(Exports),
    Abstract = [Module, ExportAttr | Functions],
    Forms = erl_syntax:revert_forms(Abstract),
    {ok, Resource, Bin} = compile:forms(Forms, [verbose, report_errors]),
    code:purge(Resource),
    code:load_binary(Resource, atom_to_list(Resource) ++ ".erl", Bin),
    ok.

make_module(Module) ->
    erl_syntax:attribute(erl_syntax:atom(module),
                         [erl_syntax:atom(Module)]).

make_export(Key) ->
    erl_syntax:arity_qualifier(erl_syntax:atom(Key),
                               erl_syntax:integer(0)).

make_export_attribute(Exports) ->
    erl_syntax:attribute(erl_syntax:atom(export),
                         [erl_syntax:list(Exports)]).

make_function(Key, Value) ->
    Constant = erl_syntax:clause([], none, [erl_syntax:abstract(Value)]),
    erl_syntax:function(erl_syntax:atom(Key), [Constant]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-compile(export_all).

assert_prop_equals(ModName, Props) ->
    ?debugFmt("Will try to load mod ~p with props ~p~n", [ModName, Props]),
    ok = sidejob_config:load_config(ModName, Props),
    [?assertEqual(apply(ModName, Key, []), proplists:get_value(Key, Props))
     || Key <- proplists:get_keys(Props)].

load_config_test() ->
    ModNames = [ list_to_atom("load_config_test_mod_"++integer_to_list(N))
                              || N <- lists:seq(1,3) ],
    PropLists = [
            [{key1, 1}, {key2, [1,2,3]}, {key3, value3}, {key4, {value, 4}}],
            [{key1, 42}, {key2, [3,2,1]}, {key3, not_value3}, {key4, {value, four}}],
            [{one, one}, {two, [2]}, {three, "three"}],
            [{one, "uno"}, {two, dos}, {three, tres}]
            ],
    [ assert_prop_equals(ModName, Props) || ModName <- ModNames, Props <- PropLists].

-endif.
