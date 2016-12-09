%% -*- erlang-indent-level: 4; indent-tabs-mode: nil -*-
%%
%% Copyright (C) 2016, Jaguar Land Rover
%%
%% This program is licensed under the terms and conditions of the
% Mozilla Public License, version 2.0.  The full text of the
%% Mozilla Public License is at https://www.mozilla.org/MPL/2.0/
%%
-module(service_handler).
-behaviour(gen_server).

-export([new/3,
	 lookup/1,
	 all/0,
	 start_link/3,
	 stop/1,
	 message/2]).

-export([init/1,
	 handle_call/3,
	 handle_cast/2,
	 handle_info/1,
	 terminate/2,
	 code_change/3]).

-record(st, {mod, mod_st, msg}).

%% -- gproc helpers
reg(SvcName) -> {n, l, {service_edge, svc, to_lower(SvcName)}}.

via(SvcName) -> {via, gproc, reg(SvcName)}.
%% --

%% -- API

new(M) ->
    supervisor:start_child(service_handler_sup, [M])

lookup(Svc) ->
    gproc:where({n, l, {rvi, service, Svc}}).

all() ->
    gproc:select({n,l}, [{ {reg('_'), '_', '_'}, [],
			   [ {element, 3, {element,1,'$_'}} ] }]).

start_link(#{} = M) ->
    gen_server:start_link(?MODULE, M, []).

stop(SvcName) ->
    gen_server:call(via(SvcName), stop).

init(#{full_service_name := Svc,
       normalized_name := Normalized,
       url = {Mod, Arg}} = Msg) ->
    case Mod:init_service(Arg) of
	{ok, MSt} ->
	    gproc:reg(reg(Normalized), local),
	    {ok, #st{msg = Msg,
		     mod = Mod,
		     mod_st = MSt}};
	Other ->
	    Other
    end.

message(Pid, #{} = Msg) when is_pid(Pid) ->
    gen_server:call(Pid, {message, Msg});
message(SvcName, #{} = Msg) ->
    gen_server:call(via(SvcName), {message, Msg}).

notification(SvcName, #{} = Msg) ->
    gen_server:call(via(SvcName), {notify, Msg}).

handle_call(stop, _From, S) ->
    {stop, normal, S};
handle_call({message, Msg}, _From, St) ->
    {Reply, St1} = message_(Msg, St),
    {reply, Reply, St1};
handle_call({notify, Msg}, From, St) ->
    gen_server:reply(From, ok),
    {_, St1} = message_(Msg, St),
    {noreply, St1};
handle_call(_, _, S) ->
    {reply, {error, unknown_call, S}}.

handle_cast(_, S) ->
    {noreply, S}.

handle_info(_, S) ->
    {noreply, S}.

terminate(_, _) ->
    ok.

code_change(_FromVsn, St, _Extra) ->
    {ok, St}.

%% ============================================================
%% Local functions
%% ============================================================

message_(Msg0, #st{url = URL, opts = Opts} = St) ->
    Msg = rvi_common:strip_local_elements(Msg0),
    case URL of
	<<"ws:" ++ WSPid/binary>> ->
	    wse_server:send(list_to_pid(WSPid), Msg);
	{Mod, MSt} ->
	    {Result, MSt1} = Mod:send_to_service(Msg, MSt),
	    {Result, St#st{url = {Mod, MSt1}}};
	<<"http" ++ _/binary>> ->
	    {ok, Result} = rvi_common:send_json_request(URL, Msg, Opts),
	    {Result, St}
    end.

to_lower(A) when is_atom(A) -> A;   % select pattern, most likely
to_lower(B) when is_binary(B) ->
    rvi_common:to_lower(B).
