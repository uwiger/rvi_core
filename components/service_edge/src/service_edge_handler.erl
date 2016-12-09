-module(service_edge_handler).
-behaviour(gen_server).

-export([start_link/3,
	 stop/1,
	 message/2]).

-export([init/1,
	 handle_call/3,
	 handle_cast/2,
	 handle_info/1,
	 terminate/2,
	 code_change/3]).

reg(SvcName) -> {n, l, {service_edge, svc, to_lower(SvcName)}}.

via(SvcName) -> {via, gproc, reg(SvcName)}.


start_link(SvcName, URL, Opts) ->
    Args = {SvcName, URL, Opts},
    gen_server:start_link(via(SvcName), ?MODULE, Args, []).

stop(SvcName) ->
    gen_server:call(via(SvcName), stop).

init({Svc, URL, Opts}) ->
    {ok, #st{svc = Svc, url = URL, opts = Opts}}.

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
	    {Result, MSt1} = Mod:handle_message(Msg, MSt),
	    {Result, St#st{url = {Mod, MSt1}}};
	<<"http" ++ _/binary>> ->
	    {ok, Result} = rvi_common:send_json_request(URL, Msg, Opts),
	    {Result, St}
    end.

to_lower(A) when is_atom(A) -> A;   % select pattern, most likely
to_lower(B) when is_binary(B) ->
    rvi_common:to_lower(B).
