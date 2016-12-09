%% -*- erlang-indent-level: 4; indent-tabs-mode: nil -*-
%%
%% Copyright (C) 2014, Jaguar Land Rover
%%
%% This program is licensed under the terms and conditions of the
%% Mozilla Public License, version 2.0.  The full text of the
%% Mozilla Public License is at https://www.mozilla.org/MPL/2.0/
%%
-module(service_edge_msg_mgr).
-behaviour(gen_server).

-export([start_link/0]).
-export([init/1,
	 handle_call/3,
	 handle_cast/2,
	 handle_info/2,
	 terminate/2,
	 code_change/3]).

-record(st, {
	  msg_tab = dlink_message_order:new()
	 }).

message(#{} = Msg) ->
    gen_server:cast(?MODULE, {msg, Msg}).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    {ok, #st{}}.

handle_cast({msg, Msg}, #st{msg_tab = T} = St) ->
    dlink_msg_order:new_handler(Msg, fun msg_handler/1, T),
    {noreply, St};
handle_cast(_, St) ->
    {noreply, St}.

handle_call(_Req, _From, St) ->
    {reply, {error, unknown_call}, St}.

handle_info(_, St) ->
    {noreply, St}.

terminate(_Reason, _St) ->
    ok.

code_change(_FromVsn, St, _Extra) ->
    {ok, St}.

%% private functions

msg_handler(#{<<"method">> := <<"message">>,
              <<"params">> := Params} = Msg) ->
    msg_handler(Method, Params, Msg).

msg_handler(<<"message">>, Params, Msg) ->
    case authorize_rpc:authorize_local_message(Params) of
        true ->
            do_handle_local_message(Msg);
        false ->
            reply({error, unauthorized}, Msg)
    end;
msg_handler(<<"register_service">>, #{<<"service_name">> := Svc,
                                      <<"opts">> := Opts}, Msg) ->
    FullSvcName = rvi_common:local_service_to_string(Svc),
    Normalized = rvi_common:to_lower(FullSvcName),
    case service_handler:new(Msg#{full_service_name => FullSvcName,
                                  normalized_name   => Normalized,
                                  conn => local}) of
        {ok, _} ->
            reply(ok, #{<<"service">> => FullSvcName}, Msg);
        {error, _} = Error ->
            reply(Error, Msg)
    end;
msg_handler(<<"unregister_service">>, #{<<"service">> := Svc}, Msg) ->
    FullSvcName = rvi_common:local_service_to_string(Svc),
    Normalized = rvi_common:to_lower(FullSvcName),
    Res = service_handler:stop(Normalized),
    reply(Res, Msg);
msg_handler(<<"get_available_services">>, _, Msg) ->
    Svcs = authorize_keys:authorized_services_for_conn(local),
    reply(ok, #{<<"services">> => Svcs}, Msg);
msg_handler(<<"get_node_service_prefix">>, Params, Msg) ->
    Prefix = rvi_common:local_service_prefix(),
    [UUID | _ ] = re:split(Prefix, <<"/">>, [{return, binary}]),
    GoodRes = fun(R) ->
                      reply(ok, #{<<"node_service_prefix">> => R}, Msg)
              end,
    case maps:find(<<"full">>, Params) of
        {ok, Full} when Full == true; Full == 1 ->
            GoodRes(Prefix);
        error ->
            GoodRes(Prefix);
        {ok, Full} when Full == false; Full == 0 ->
            GoodRes(UUID);
        _ ->
            reply({error, invalid}, Msg)
    end.

do_handle_local_message(#{<<"params">> :=
                              #{<<"service_name">> := SvcName,
                                <<"timeout">> := TimeoutArg0,
                                <<"parameters">> := Parameters}} = Msg) ->
    Normalized = rvi_common:to_lower(SvcName),
    Msg1 = Msg#{<<"timeout">> => timeout_arg(TimeoutArg0),
                normalized_name => Normalized},
    %%
    %% Check if this is a local service by trying to resolve its service name.
    %% If successful, just forward it to its service_name.
    %%
    LookupRes = service_handler:lookup(Normalized),
    ?debug("Service LookupRes = ~p", [LookupRes]),
    TimeoutMS = timeout_ms(TimeoutArg),
    ?debug("TimeoutMS = ~p", [TimeoutMS]),
    case LookupRes of
        Pid when is_pid(Pid) ->
            ?debug("local_msg(): Service is local. Forwarding."),
            log("dispatch to ~s", [URL], Msg1),
            ?event({matching_service_entry, E}),
            Res = forward_message_to_local_service(URL, Msg1),
            rpc_return(local, Msg1, TimeoutMS, Res);
        _ ->
            %% SvcName is remote
            %% Ask Schedule the request to resolve the network address
            maybe_register_reply_id(Msg1),
            ?debug("local_msg(): Service is remote. Scheduling."),
            log("schedule message (~s)", [SvcName], Msg1),
            {ok, TID} = schedule_rpc:schedule_message(Msg1),
            rpc_return(remote, Msg1, TimeoutMS, {ok, TID})
    end.

reply(Res, Msg) ->
    reply(Res, #{}, Msg).

reply(Res, M, #{<<"method">> := Method,
                <<"id">> := Id,
                reply_to := ReplyTo}) ->
    {Code, Str} = rvi_common:json_rpc_status_t(status(Res)),
    M1 = M#{<<"status">> => Code,
            <<"message">> => Str,
            <<"method">>  => Method},
    Reply = M#{<<"jsonrpc">> => <<"2.0">>,
               <<"id">>      => Id,
               <<"result">>  => M1},
    case ReplyTo of
        {wse, WSock} ->
            Enc = jsx:encode(Reply),
            wse_server:send(WSock, Enc);
        Pid when is_pid(Pid) ->
            Pid ! {rpc_reply, Reply}
    end.

rpc_return(Type, Msg, Timeout, DefaultRes) ->
    foo.

lookup_service(Svc) ->
