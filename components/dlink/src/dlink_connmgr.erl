%% -*- erlang-indent-level: 4; indent-tabs-mode: nil -*-
%%
%% Copyright (C) 2016, Jaguar Land Rover
%%
%% This program is licensed under the terms and conditions of the
%% Mozilla Public License, version 2.0.  The full text of the
%% Mozilla Public License is at https://www.mozilla.org/MPL/2.0/
%%
%%
%%%-------------------------------------------------------------------
%%% @author magnus <magnus@t520.home>
%%% @copyright (C) 2014, magnus
%%% @doc
%%%
%%% @end
%%% Created : 28 Mar 2016 by Ulf Wiger <ulf@wiger.net>
%%%-------------------------------------------------------------------
-module(dlink_connmgr).

-behaviour(gen_server).
-include_lib("lager/include/log.hrl").

%% API
-export([start_link/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-export([add_connection/4]).
-export([delete_connection_by_pid/2]).
-export([delete_connection_by_address/3]).
-export([find_connection_by_pid/2]).
-export([find_connection_by_address/3]).
-export([connections/1]).

-define(SERVER, ?MODULE).

-record(st, {
          mod,
          mod_st,
          parent,
          addr_tab,
          pid_tab,
          cs}).

%%%===================================================================
%%% API
%%%===================================================================

add_connection(Mgr, IP, Port, Pid) ->
    gen_server:call(Mgr, { add_connection, IP, Port, Pid }).

delete_connection_by_pid(Mgr, Pid) ->
    gen_server:call(Mgr, { delete_connection_by_pid, Pid } ).

delete_connection_by_address(Mgr, IP, Port) ->
    gen_server:call(Mgr, { delete_connection_by_address, IP, Port } ).

find_connection_by_pid(Mgr, Pid) ->
    gen_server:call(Mgr, { find_connection_by_pid, Pid } ).

find_connection_by_address(Mgr, IP, Port) ->
    gen_server:call(Mgr, { find_connection_by_address, IP, Port } ).

connections(Mgr) ->
    gen_server:call(Mgr, connections).

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link(Opts) ->
    [Name, Module, Pids, Addrs] = [opt(K, Opts)
                                   || K <- [name, module, pids, addrs]],
    Arg = proplists:get_value(arg, Opts, []),
    create_ets(Pids, Addrs),
    gen_server:start_link({local, Name}, ?MODULE,
                          {self(), Module, Arg, Pids, Addrs}, []).

opt(K, L) ->
    case lists:keyfind(K, 1, L) of
        {_, V} -> V;
        false  -> error({required, K})
    end.

create_ets(Pids, Addrs) ->
    maybe_create(Pids),
    maybe_create(Addrs).

maybe_create(Tab) ->
    case ets:info(Tab, name) of
        undefined ->
            ets:new(Tab, [public, named_table, set]),
            Tab;
        _ ->
            Tab
    end.

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
init({Parent, Module, Arg, PidTab, AddrTab}) ->
    {ok, ModSt} = Module:init(Arg),
    {ok, #st{mod = Module,
             mod_st = ModSt,
             parent = Parent,
             addr_tab = AddrTab,
             pid_tab = PidTab,
             cs = rvi_common:get_component_specification()}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @spec handle_call(Request, From, State) ->
%%                                   {reply, Reply, State} |
%%                                   {reply, Reply, State, Timeout} |
%%                                   {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, Reply, State} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_call({add_connection, IP, Port, Pid}, _From, #st{mod = M,
                                                        addr_tab = ATab,
                                                        pid_tab = PTab} = St) ->
    Addr = {IP, Port},
    ?debug("~p:handle_call(add): Adding Pid: ~p, Address: ~p",
             [ ?MODULE, Pid, Addr]),
    %% Store so that we can find connection both by pid and by address
    erlang:monitor(process, Pid),
    ets_insert(PTab, {Pid, Addr}),
    ets_insert(ATab, {Addr, Pid}),
    {reply, ok, St};

%% Delete connection by pid
handle_call({delete_connection_by_pid, Pid}, _From, #st{} = St)
  when is_pid(Pid) ->
    Res = delete_connection_by_pid_(Pid, St),
    {reply, Res, St};

%% Delete connection by address
handle_call({delete_connection_by_address, IP, Port},
            _From, #st{mod = M, pid_tab = PTab, addr_tab = ATab} = St) ->
    %% Find Pid associated with Address
    Addr = {IP, Port},
    case ets_lookup(ATab, Addr) of
        [] ->
            ?debug("~p:handle_call(del_by_addr): not found: ~p",
                   [ ?MODULE, {IP, Port}]),
            { reply, not_found, St};
        [{_, Pid}] ->
            ?debug("~p:handle_call(del_by_addr): deleted Pid: ~p, Address: ~p",
                   [ ?MODULE, Pid, {IP, Port}]),
            ets_delete(PTab, Pid),
            ets_delete(ATab, Addr),
            {reply, ok, St}
    end;

%% Find connection by pid
handle_call({find_connection_by_pid, Pid}, _From,
            #st{pid_tab = PTab} = St) when is_pid(Pid)->
    %% Find address associated with Pid
    case ets_lookup(PTab, Pid) of
        [] ->
            ?debug("~p:handle_call(find_by_pid): not found: ~p~n~p",
                   [ ?MODULE, Pid, ets:tab2list(PTab)]),
            {reply, not_found, St};
        [{_, {IP, Port}}] ->
            ?debug("~p:handle_call(find_by_addr): Pid: ~p ->: ~p",
                   [ ?MODULE, Pid, {IP, Port}]),
            {reply, {ok, IP, Port}, St}
    end;

%% Find connection by address
handle_call({find_connection_by_address, IP, Port}, _From,
            #st{addr_tab = ATab} = St) ->
    %% Find address associated with Pid
    Addr = {IP, Port},
    case ets_lookup(ATab, Addr) of
        [] ->
            ?debug("~p:handle_call(find_by_addr): not found: ~p",
                   [ ?MODULE, Addr]),
            { reply, not_found, St};
        [{_, Pid}] ->
            ?debug("~p:handle_call(find_by_addr): Addr: ~p ->: ~p",
                   [ ?MODULE, Addr, Pid]),
            {reply, {ok, Pid}, St}
    end;

handle_call(connections, _From, #st{addr_tab = ATab} = St) ->
    {reply, ets_select(ATab, [{ {'$1','_'}, [], ['$1'] }]), St};

handle_call(_Request, _From, #st{} = St) ->
    ?warning("~p:handle_call(): Unknown call: ~p", [ ?MODULE, _Request]),
    Reply = ok,
    {reply, Reply, St}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                  {noreply, State, Timeout} |
%%                                  {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_cast(_Msg, #st{} = St) ->
    ?warning("~p:handle_cast(): Unknown call: ~p", [ ?MODULE, _Msg]),
    {noreply, St}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_info({'DOWN', _Ref, process, Pid, _}, #st{} = St) ->
    delete_connection_by_pid_(Pid, St),
    {noreply, St};
handle_info(_Info, #st{} = St) ->
    ?warning("~p:handle_cast(): Unknown info: ~p", [ ?MODULE, _Info]),
    {noreply, St}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

ets_lookup(Tab, Key) ->
    ets:lookup(Tab, Key).

ets_insert(Tab, Obj) ->
    ets:insert(Tab, Obj).

ets_delete(Tab, Key) ->
    ets:delete(Tab, Key).

ets_select(Tab, Pat) ->
    ets:select(Tab, Pat).

delete_connection_by_pid_(Pid, #st{cs = CS,
                                   addr_tab = ATab,
                                   pid_tab = PTab}) ->
    case ets_lookup(PTab, Pid) of
        [] ->
            ?debug("del_by_pid: not found: ~p", [ Pid]),
            not_found;
        [{_, Addr}] ->
            ?debug("del_by_pid: deleted Pid: ~p, Address: ~p", [Pid, Addr]),
            ets_delete(PTab, Pid),
            ets_delete(ATab, Addr),
            authorize_rpc:remove_connection(CS, Addr),
            ok
    end.
