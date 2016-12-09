%% -*- erlang-indent-level: 4; indent-tabs-mode: nil -*-
%%
%% Copyright (C) 2014-2016, Jaguar Land Rover
%%
%% This program is licensed under the terms and conditions of the
% Mozilla Public License, version 2.0.  The full text of the
%% Mozilla Public License is at https://www.mozilla.org/MPL/2.0/
%%


-module(service_edge_rpc).
-behaviour(gen_server).

-export([handle_rpc/3]).
-export([handle_notification/2]).
-export([ws_init/1,
	 ws_message/2]).


-export([start_link/0,
         register_service/2]).

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-export([rpc/2,     %% (Service, Params)
         rpc/3,     %% (Service, Timeout, Params)
         msg/2,     %% (Service, Params)
         msg/3,     %% (Service, Timeout, Params)
	 reply_to_client/2
        ]).

-export([handle_remote_message/5,
         handle_local_timeout/3]).

-export([start_json_server/0,
         start_websocket/0]).

%% exo_socket authentication callbacks
-export([authenticate/3,
         incoming/2]).

%% Invoked by service discovery
%% FIXME: Should be rvi_service_discovery behavior
-export([service_available/3,
         service_unavailable/3]).

-export([record_fields/1]).

%%-include_lib("lhttpc/include/lhttpc.hrl").
-include_lib("lager/include/log.hrl").

-include_lib("rvi_common/include/rvi_common.hrl").

-include_lib("trace_runner/include/trace_runner.hrl").

-define(SERVER, ?MODULE).
-define(LONG_TIMEOUT, 60000).

-record(st, {
          %% Component specification
          cs = #component_spec{},
          pending = [],
          msg_tab = dlink_msg_order:init()
         }).

-record(ws, {
	 }).

-define(SERVICE_TABLE, rvi_local_services).

-record(service_entry, {
          key = "",
          service = "",       %% Service handled by this entry.
          url = undefined,    %% URL where the service can be reached.
          opts = []
         }).

record_fields(service_entry)    -> record_info(fields, service_entry);
record_fields(st           )    -> record_info(fields, st);
record_fields(component_spec)   -> record_info(fields, component_spec);
record_fields(_)                -> no.

rpc(Service, Args) ->
    rpc(Service, 10000, Args).

rpc(Service, Timeout, Args) ->
    handle_rpc_(
      <<"message">>,
      validate(message,
               [{<<"service_name">>, service_name(Service)},
                {<<"timeout">>, Timeout},
                {<<"synch">>, true},
                {<<"parameters">>, Args}])).

msg(Service, Args) ->
    msg(Service, 10000, Args).

msg(Service, Timeout, Args) ->
    handle_rpc_(
      <<"message">>,
      validate(message,
               [{<<"service_name">>, service_name(Service)},
                {<<"timeout">>, Timeout},
                {<<"parameters">>, Args}])).

service_name(<<"$PFX", Rest/binary>>) ->
    Pfx = rvi_common:local_service_prefix(),
    re:replace(<<Pfx/binary, Rest/binary>>, <<"//">>, <<"/">>,
               [global, {return, binary}]);
service_name(Svc) ->
    Svc.

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

init([]) ->
    CompSpec = rvi_common:get_component_specification(),

    URL = rvi_common:get_module_json_rpc_url(
            service_edge, ?MODULE, CompSpec),
    ?notice("---- Service Edge URL:          ~p", [URL]),
    ets:new(?SERVICE_TABLE, [set, public, named_table,
                             {keypos, #service_entry.key}]),
    service_discovery_rpc:subscribe(?MODULE),
    {ok, #st{
            cs = CompSpec
           }}.

register_service(SvcName, URL) ->
    ?debug("register_service()~n", []),
    handle_rpc_(<<"register_service">>,
                validate(register_service, [{<<"service">>, SvcName},
                                            {<<"network_address">>, URL}])).


%% Invoked by service_discovery to announce service availability
%% Must be handled either as a JSON-RPC call or a gen_server call.
service_available(CompSpec, SvcName, DLMod) ->
    ?event({service_available, SvcName}),
    gen_server:cast(?MODULE, {rvi, service_available,
                              #{<<"service">>          => SvcName,
                                <<"data_link_module">> => DLMod}}).

service_unavailable(CompSpec, SvcName, DLMod) ->
    ?event({service_unavailable, SvcName}),
    gen_server:cast(?MODULE, {rvi, service_unavailable,
                              #{<<"service">>          => SvcName,
                                <<"data_link_module">> => DLMod}}).

handle_remote_message(CompSpec, Conn, SvcName, Options, Params) ->
    ?event({handle_remote_message, [Conn, SvcName, Options, Params]}),
    {IP, Port} = Conn,
    gen_server:cast(?MODULE, {rvi, handle_remote_message,
                              #{<<"ip">>         => IP,
                                <<"port">>       => Port,
                                <<"service">>    => SvcName,
                                <<"options">>    => Options,
                                <<"parameters">> => Params}}).

%% Invoked by schedule_rpc.
%% A message originated from a locally connected service
%% has timed out
handle_local_timeout(CompSpec, SvcName, TransID) ->
    ?event({handle_local_timeout, [SvcName, TransID]}),
    rvi_common:notification(service_edge, ?SERVER, handle_local_timeout,
                            [ { service, SvcName},
                              { transaction_id, TransID} ],
                            CompSpec).

%% ============================================================
%% == JSON Server

start_json_server() ->
    Allowed = get_allowed(),
    ?debug("service_edge_rpc:start_json_server()"),
    Opts = [{exo_socket, [{auth, [ {role, server},
				   {server,
				    [{mod, ?MODULE},
				     {allowed, Allowed}]} ]}
			 ]}],
    case rvi_common:start_json_rpc_server(
	   service_edge, ?MODULE, service_edge_sup, Opts) of
        ok ->
	    ok;
        Err ->
            ?warning("start_json_server(): Failed to start: ~p", [Err]),
            Err
    end.

get_allowed() ->
    Allowed = case rvi_common:get_module_config(
                     service_edge, service_edge_rpc, allowed) of
                  {ok, L}    -> L;
                  {error, _} -> [{127,0,0,1}]
        end,
    ?debug("get_allowed(); Allowed = ~p", [Allowed]),
    lists:flatmap(
      fun(Addr) ->
              case inet:ip(Addr) of
                  {ok, IP}   -> [IP];
                  {error, _} -> []
              end
      end, Allowed).

authenticate(X, Role, Opts) ->
    {ok, {PeerIP,_}} = exo_socket:peername(X),
    ?debug("authenticate(~p, ~p, ~p)~nPeer = ~p~n", [X, Role, Opts, PeerIP]),
    case lists:keyfind(allowed, 1, Opts) of
        {_, Allowed} ->
            case lists:member(PeerIP, Allowed) of
                true ->
                    {ok, Opts};
                false ->
                    error
            end;
        false ->
            {ok, Opts}
    end.

incoming(Data, _) -> Data.

%% Callback for JSON-RPC request
%%
handle_rpc(Method, Id, Args0) ->
    case validate(Method, Args0) of
	{ok, Args} ->
	    Msg = #{<<"method">> => Method,
		    <<"id">>     => Id,
		    <<"params">> => Args,
		    client_ref     => self()},
	    handle_rpc_(Msg);
	{error, _} = Error ->
	    Error
    end.

handle_rpc_(Msg) ->
    Timeout = calc_timeout(Msg),
    service_edge_msg_mgr:message(Msg),
    receive
	{rpc_reply, Reply} ->
	    {ok, Reply}
    after Timeout ->
	    {Code, EMsg} = rvi_common:json_rpc_status_t(timeout),
	    {ok, #{<<"code">> => Code,
		   <<"message">> => EMsg}}
    end.

calc_timeout(#{<<"params">> => #{<<"timeout">> := T}}) ->
    Timeout = timeout_arg(T),
    timeout_ms(Timeout).

%% ============================================================
%% == Websocket Server

start_websocket() ->
    %%
    %% Fire up the websocket subsystem, if configured
    %%
    case rvi_common:get_module_config(
	   service_edge, service_edge_rpc, websocket, not_found) of
        {ok, not_found} ->
            ?notice("service_edge:init(): "
                    "No websocket config specified. "
                    "Will use JSON-RPC/HTTP only."),
            ok;
        {ok, WSOpts} ->
            case proplists:get_value(port, WSOpts) of
                undefined ->
                    ok;
                Port ->
                    %% FIXME: MONITOR AND RESTART
                    wse_server:start(
                      Port, ?MODULE, [],
                      [{type, text} | proplists:delete(port, WSOpts)]),
                    ok
            end
    end.

ws_init([]) ->
    {ok, #ws{}}.

ws_message(Msg, #ws{}) ->
    handle_websocket(_WSock = self(), Msg),
    {ok, S}.

handle_websocket(WSock, Msg) ->
    try jsx:decode(Msg, [return_maps]) of
	Decoded ->
	    handle_websocket_dec(WSock, Decoded)
    catch error:E0 ->
	    ?debug("Failed decode of ~p: ~p", [Msg, E0]),
	    ws_error(WSock, parse_error)
    end.

handle_websocket_dec(WSock, Decoded) ->
    ?event({handle_websocket, Decoded}),
    ?debug("Decoded Mesg = ~p", [Decoded]),
    case rpc_params(Decoded) of
        {ok, [Method, Params0 | ID0]} ->
	    case validate(Method, Params0) of
		#{} = Params ->
		    service_edge_msg_mgr:message(
		      set_client_ref(Id0, WSock,
				     #{<<"method">>  => Method,
				       <<"params">>  => Params})),
		    ok;
		{error, Err} ->
		    ws_error(WSock, Err)
	    end;
        {error, _} ->
	    ws_error(WSock, invalid)
    end.

rpc_params(#{<<"method">> := M, <<"params">> := Ps} = Msg) ->
    case maps:find(<<"id">>, Msg) of
	{ok, ID} -> {ok, [M, Ps, ID]};
	error    -> {ok, [M, Ps]}
    end;
rpc_params(_) ->
    {error, invalid}.

set_client_ref([]  , _    , M)   -> M;
set_client_ref([Id], WSock, M) -> M#{<<"id">> => Id,
				     client_ref => {ws, WSock}}.

reply_to_client(Reply, #{<<"id">> := Id,
			 client_ref := CRef}) ->
    %% FIXME: WS and HTTP behaviors should be symmetrical
    case CRef of
	Pid when is_pid(Pid) ->
	    Pid ! {rpc_reply, Reply};
	{ws, WSPid} ->
	    wse_server:send(WSPid, #{<<"jsonrpc">> => <<"2.0">>,
				     <<"id">> => Id,
				     <<"result">> => Reply})
    end.

ws_error(WSock, Error) ->
    {Code, EMsg} = rvi_common:json_rpc_status_t(Err),
    JSON = #{<<"jsonrpc">> => <<"2.0">>,
	     <<"error">>   => #{<<"code">> => Code,
				<<"message">> => EMsg}},
    Enc = jsx:encode(JSON),
    wse_server:send(WSock, Enc).

%% ============================================================

handle_ws_json_rpc(WSock, Method, Params, ID, S) ->
    case handle_ws_json_rpc(WSock, Method, Params#{rpc_id = ID}, Arg) of
        ok -> ok;
        {ok, #{} = R0} ->
            R = rvi_common:strip_local_elements(R0),
            EncReply = jsx:encode(R#{<<"id">> => ID}),
            ?debug("handle_websocket(~p/~p) reply:      ~s",
                   [WSock, ID, EncReply]),
            wse_server:send(WSock, EncReply)
    end,
    ok.

%% Websocket interface
handle_ws_json_rpc(WSock, <<"message">> = M, Params0, S) ->
    ?debug("ws_json_rpc(~p, ~p)", [M, Params0]),
    handle_rpc_(<<"message">>, validate(message, Params0),
		#{from => {ws, WSock}});
handle_ws_json_rpc(WSock, <<"register_service">> = M, Params, _Arg) ->
    case validate(register_service, Params) of
        {ok, R} ->
            do_register_service(R#{url = {?MODULE, {ws, WSock}}});
        {error, E} ->
            validation_error(M, E)
    end;
    %% PBin = list_to_binary(pid_to_list(WSock)),
    %% URL = <<"ws:", PBin/binary>>,
    %% handle_rpc_(
    %%   <<"register_service">>,
    %%   validate(register_service, Params),
    %%   #{<<"url">> => URL});
handle_ws_json_rpc(WSock, R, Params, _Arg) ->
    handle_rpc_(R, validate(R, Params), #{from => {ws, WSock}}).
%% handle_ws_json_rpc(WSock, <<"unregister_service">>, Params, _Arg ) ->
%%     { ok, SvcName } = rvi_common:get_json_element(["service_name"], Params),
%%     ?event({unregister_service, ws, SvcName}),
%%     ?debug("service_edge_rpc:websocket_unregister(~p) service:    ~p", [ WSock, SvcName ]),
%%     gen_server:call(?SERVER, { rvi, unregister_service, [ SvcName ]}),
%%     { ok, [ { status, rvi_common:json_rpc_status(ok)} ]};

%% handle_ws_json_rpc(WSock, <<"get_node_service_prefix">>, Params, _Arg) ->
%%     ?debug("websocket_get_node_service_prefix(~p)", [WSock]),
%%     get_node_service_prefix_(Params);

%% handle_ws_json_rpc(_Ws , <<"get_available_services">>, _Params, _Arg ) ->
%%     ?debug("service_edge_rpc:websocket_get_available()"),
%%     [ ok, Services ] =
%%      gen_server:call(?SERVER, { rvi, get_available_services, []}),
%%     { ok, [ { status, rvi_common:json_rpc_status(ok)},
%%          { services, Services},
%%          { method, <<"get_available_services">>}] }.

%% Invoked by locally connected services.
%% Will always be routed as JSON-RPC since that, and websocket,
%% are the only access paths in.
%%
handle_rpc(Method, Args) ->
    handle_rpc_(Method, validate(Method, Args), #{}).

%% handle_rpc(<<"get_available_services">>, _Args) ->
%%     [ Status, Services ] = gen_server:call(?SERVER, { rvi, get_available_services, []}),

%%     ?debug("get_available_services(): ~p ~p", [ Status, Services ]),
%%     {ok, [ { status, rvi_common:json_rpc_status(ok)},
%%         { services, Services},
%%         { method, <<"get_available_services">>}
%%       ]};

%% handle_rpc(<<"message">>, Args) ->
%%     ?event({message, json_rpc, Args}),
%%     handle_rpc_(message, validate(message, Args));

%% handle_rpc(Other, _Args) ->
%%     ?warning("service_edge_rpc:handle_rpc(~p): unknown command", [ Other ]),
%%     {ok,[ { status, rvi_common:json_rpc_status(invalid_command)} ]}.

validation_error(M, Err) ->
    ?debug("validation failed (~p): ~p", [M, Err]),
    {error, invalid_params}.

handle_rpc_(M, {error, Err}, _XArgs) ->
    validation_error(M, Err);
handle_rpc_(<<"message">> = M, {ok, Args0}, XArgs) ->
    Msg = maps:merge(XArgs, Args0),
    ?debug("message: Msg = ~p", [Msg]),
    case gen_server:call(
           ?SERVER, {rvi, message, Args}, ?LONG_TIMEOUT) of
        [Res, TID] ->
            ?debug("'message' result: ~p", [[Res, TID]]),
            {ok, [ { status, rvi_common:json_rpc_status(Res) },
                   { transaction_id, TID },
                   { method, <<"message">>}
                 ]};
        [Res] ->
            {ok, [ { status, rvi_common:json_rpc_status(Res) } ]};
        {ok, _} = CompleteResult ->
            CompleteResult
    end;
handle_rpc_(M, {ok, Args0}, XArgs) ->
    Args = maps:merge(XArgs, Args0),
    gen_server:call(?SERVER, {rvi, M, Args}).

get_node_service_prefix_(Params) ->
    Prefix = rvi_common:local_service_prefix(),
    [UUID | _ ] = re:split(Prefix, <<"/">>, [{return, binary}]),
    GoodRes = fun(R) ->
                      { ok, [ { status, rvi_common:json_rpc_status(ok) },
                              { node_service_prefix, R },
                              { method, <<"get_node_service_prefix">> } ]}
              end,
    case rvi_common:get_json_element(["full"], Params) of
        {ok, Full} when Full == true; Full == 1 ->
            GoodRes(Prefix);
        {error, _} ->
            GoodRes(Prefix);
        {ok, Full} when Full == false; Full == 0 ->
            GoodRes(UUID);
        _ ->
            { ok, [ { status, rvi_common:json_rpc_status(invalid_command) },
                    { method, <<"get_node_service_prefix">> } ] }
    end.

handle_notification(R, Args) ->
    handle_notification_(R, validate(R, Args)).

handle_notification_(R, {ok, Args}) ->
    gen_server:cast(?SERVER, {rvi, R, Args});
handle_notification_(R, {error, Err}) ->
    {error, invalid_params}.

%% Handle calls received through regular gen_server calls, routed by
%% rvi_common:request() We only need to implement
%% register_remote_services() and handle_remote_message Since they are
%% the only calls invoked by other components, and not the locally
%% connected services that uses the same HTTP port to transmit their
%% register_service, and message calls.
handle_call(Req, From, St) ->
    R = add_method(Req),
    try handle_call_(Req, From, R, St)
    catch
        throw:Reason ->
            {reply, R#{status => Reason}, St};
        error:Error ->
            {reply, R#{status => internal_error}, St}
    end.


add_method(Req) -> add_method(Req, #{}).

add_method({rvi, M, _}, Res) when is_atom(M) ->
    Res#{method => atom_to_binary(M, latin1)};
add_method({rvi, M, _}, Res) when is_binary(M) ->
    Res#{method => M};
add_method(_, Res) ->
    Res.

handle_call_({rvi, register_service,
              #{<<"service">> := Svc,
                <<"network_address">> := URL,
                <<"opts">> := Opts}}, _From, R, St) ->
    ?debug("service_edge_rpc:register_service(): service:   ~p ",   [Svc]),
    ?debug("service_edge_rpc:register_service(): address:   ~p ",   [URL]),

    FullSvcName = rvi_common:local_service_to_string(Svc),
    try register_local_service_(FullSvcName, URL, Opts, R, St)
    catch
        throw:Reason ->
            {reply, {error, Reason}, St}
    end;

handle_call_({rvi, unregister_service, #{<<"service">> := Svc}}, _, R, St) ->
    ?debug("service_edge_rpc:unregister_service(): service: ~p ", [Svc]),


    ets:delete(?SERVICE_TABLE, Svc),

    %% Register with service discovery, will trigger callback to service_available()
    %% that forwards the registration to other connected services.
    R1 = start_log("unreg local service: ~s", [Svc], R),
    service_discovery_rpc:unregister_services([Svc], local),

    %% Return ok.
    {reply, R1#{status => ok}, St};



handle_call_({rvi, get_available_services, R}, _From, R, St) ->
    ?debug("service_edge_rpc:get_available_services()"),
    Svcs = service_discovery_rpc:get_all_services(),
    {reply, R#{status => ok,
               services => Svcs}, St};

%%CRASH13:43:57.370 [debug] service_edge_rpc:local_msg: parameters:      [{struct,"{"value":"3"}"}]

%%13:43:57.370 [debug] service_edge_rpc:local_msg: parameters:      [{struct,"{"value":"3"}"}]
%% [{struct,[{"a","b"}]}]
%% 13:48:12.943 [debug] service_edge_rpc:local_msg: parameters:      [{struct,[{"a","b"}]}]

handle_call_({rvi, message, #{<<"service_name">> := SvcName} = R},
             From, R, #st{pending = Pend, msg_tab = T} = St) ->
    R1 = start_log("local_message: ~s", [SvcName], R),
    %%
    %% Authorize local message and retrieve a certificate / signature
    %% that will be accepted by the receiving node that will deliver
    %% the message to its locally connected service_name service.
    %%
    {St1, R2} =
        case check_if_synch(R1#{from => From}) of
            {true, R11} ->
                {Pid, Ref} =
                    spawn_monitor(
                      fun() ->
                              exit({deferred_reply, await_rpc_reply(R11)})
                      end),
                {St#st{pending = [{Pid, Ref, R11, From}|Pend]}, R11};
            false ->
                {St, R1}
        end,
    dlink_msg_order:new_handler(R2, fun msg_handler/1, T),
    {noreply, St1};
    %% {Pid, Ref} =
    %%  spawn_monitor(
    %%    fun() ->
    %%            exit({deferred_reply, handle_local_message_(R1)})
    %%    end),
    %% {noreply, St#st{pending = [{Pid, Ref, R1, From}|Pend]}};

handle_call_(Other, _From, _R, St) ->
    ?warning("service_edge_rpc:handle_call(~p): unknown", [ Other ]),
    { reply, {error, invalid_command}, St}.



handle_cast({rvi, service_available,
             #{<<"service_name">> := SvcName} = R}, St) ->
    ?debug("service_edge_rpc: Service available: ~p", [SvcName]),
    start_log("service_available: ~s", [SvcName], R),
    announce_service_availability(available, SvcName),
    {noreply, St};

handle_cast({rvi, service_unavailable,
             #{<<"service_name">> := SvcName} = R}, St) ->
    ?debug("service_edge_rpc: Service unavailable: ~p:", [ SvcName]),
    start_log("service_unavailable: ~s", [SvcName], R),
    announce_service_availability(unavailable, SvcName),
    {noreply, St};

handle_cast({rvi, handle_remote_message,
             #{<<"service_name">> := SvcName} = R}, #st{} = St) ->
    ?event({handle_remote_message, [R]}, St),
    case SvcName of
        <<"$RVI/", _/binary>> ->
            dispatch_reply(SvcName, R);
        _ ->
            spawn(fun() ->
                          handle_remote_message_(R)
                  end)
    end,
    {noreply, St};

handle_cast({rvi, handle_local_timeout, [SvcName, TransactionID] }, St) ->
    %% FIXME: Should be forwarded to service.
    ?info("service_edge_rpc:handle_local_timeout(): service: ~p trans_id: ~p ",
          [SvcName, TransactionID]),

    {noreply, St};

handle_cast(Other, St) ->
    ?warning("service_edge_rpc:handle_cast(~p): unknown", [ Other ]),
    {noreply, St}.

handle_info({'DOWN', _Ref, _, _, {msg_handler_done, Key, Res}},
            #st{msg_tab = T} = St) ->
    case dlink_msg_order:handler_done(Key, fun msg_handler/1, T) of
        #{from := From} ->
            gen_server:reply(From, Res);
        _ ->
            ignored
    end,
    {noreply, St};
handle_info({'DOWN', Ref, _, _, {deferred_reply, Deferred}},
             #st{pending = Pend} = St) ->
    ?debug("got deferred reply: ~p", [Deferred]),
    case lists:keyfind(Ref, 2, Pend) of
        {_Pid, Ref, R, From} = P ->
            Reply =
                case Deferred of
                    [ok, {_, <<"{\"jsonrpc\"", _/binary>> = JSON}] ->
                        Decoded = jsx:decode(JSON),
                        ?debug("Decoded = ~p", [Decoded]),
                        {_, Res} = lists:keyfind(<<"result">>, 1, Decoded),
                        ?debug("Res = ~p", [Res]),
                        {ok, lists:foldl(
                               fun convert_status/2, R, Res)};
                    [ok, [{_,_}|_] = ReplyL] ->
                        case lists:keyfind(<<"result">>, 1, ReplyL) of
                            {_, Res} ->
                                ?debug("Res = ~p", [Res]),
                                {ok, lists:foldl(
                                       fun convert_status/2, R, Res)};
                            false ->
                                ?debug("Cannot find result: ~p", [Deferred]),
                                Deferred
                        end;
                    [ok, I] when is_integer(I) ->
                        Deferred;
                    Other ->
                        ?debug("Strange deferred_reply: ~p", [Other]),
                        Other
                end,
            ?debug("Reply = ~p", [Reply]),
            gen_server:reply(From, Reply),
            {noreply, St#st{pending = Pend -- [P]}};
        false ->
            {noreply, St}
    end;
handle_info({'DOWN', Ref, _, _, Reason}, #st{pending = Pend} = St) ->
    case lists:keyfind(Ref, 2, Pend) of
        {Pid, _, From} = P ->
            ?error("~p died: ~p", [Pid, Reason]),
            gen_server:reply(From, [internal]),
            {noreply, St#st{pending = Pend -- [P]}};
        false ->
            ?debug("got DOWN, but no corresponding pending", []),
            {noreply, St}
    end;
handle_info(_Info, St) ->
    {noreply, St}.

convert_status({<<"status">> = K, St}, R) ->
    R#{K => rvi_common:json_rpc_status(St)};
convert_status({K, V}, R) ->
    R#{K => V}.

terminate(_Reason, _St) ->
    ok.
code_change(_OldVsn, St, _Extra) ->
    {ok, St}.

dispatch_reply(<<"rvi:", _/binary>> = ReplyId, Params) ->
    case gproc:where({n, l, {rvi, rpc, ReplyId}}) of
        undefined ->
            ?debug("No process matching ~p", [ReplyId]),
            ignore;
        Pid ->
            Pid ! {rvi, rpc_return, ReplyId, Params},
            ok
    end.

handle_remote_message_(#{<<"service_name">> := SvcName} = R) ->
    ?debug("remote_msg(): ~p", [R]),
    case lookup_service(SvcName) of
        [#service_entry{url = URL}] ->   % local message
            case authorize_rpc:authorize_remote_message(R) of
                ok ->
                    forward_remote_msg_to_local_service(URL, R);
                {error, _} = Err ->
                    ?debug("remote_msg(): Failed to authenticate ~p (~p)",
                           [R, Err])
            end;
        [] ->
            case service_discovery_rpc:is_service_available(SvcName) of
                false ->
                    ?debug("Remote service unavailable (~p)", [SvcName]);
                true ->
                    {_, R1} = check_if_synch(R),
                    schedule_rpc:schedule_message(R1)
            end
    end.

lookup_service(SvcName) ->
    ets:lookup(?SERVICE_TABLE, to_lower(SvcName)).

%% This function runs inside a newly spawned process
msg_handler(#{} = R) ->
    case authorize_rpc:authorize_local_message(R) of
        true ->
            do_handle_local_message(R);
        false ->
            {error, unauthorized}
    end.

maybe_register_reply_id(#{<<"reply_id">> := ReplyId}) ->
    gproc:reg({n,l,{rvi, rpc_waiter, ReplyId}});
maybe_register_reply_id(_) ->
    false.

do_handle_local_message(#{<<"service_name">> := SvcName,
                          <<"timeout">> := TimeoutArg0,
                          <<"parameters">> := Parameters} = R) ->
    TimeoutArg = timeout_arg(TimeoutArg0),
    R1 = strip_local_elems(R#{<<"timeout">> => TimeoutArg}),
    %%
    %% Check if this is a local service by trying to resolve its service name.
    %% If successful, just forward it to its service_name.
    %%
    LookupRes = lookup_service(SvcName),
    ?debug("Service LookupRes = ~p", [LookupRes]),
    TimeoutMS = timeout_ms(TimeoutArg),
    ?debug("TimeoutMS = ~p", [TimeoutMS]),
    case LookupRes of
        [#service_entry{url = URL} = E] ->
            ?debug("local_msg(): Service is local. Forwarding."),
            log("dispatch to ~s", [URL], R1),
            ?event({matching_service_entry, E}),
            Res = forward_message_to_local_service(URL, R1),
            rpc_return(local, R1, TimeoutMS, Res);
        _ ->
            %% SvcName is remote
            %% Ask Schedule the request to resolve the network address
            maybe_register_reply_id(R1),
            ?debug("local_msg(): Service is remote. Scheduling."),
            log("schedule message (~s)", [SvcName], R1),
            {ok, TID} = schedule_rpc:schedule_message(R1),
            rpc_return(remote, R1, TimeoutMS, {ok, TID})
    end.

strip_local_elems(#{} = R) ->
    maps:filter(fun(K, _) ->
                        is_binary(K)
                end, R).

check_if_synch(#{<<"synch">> := IsSynch0} = R) ->
    IsSynch = case IsSynch0 of
                  true          -> true;
                  <<"true">>    -> true;
                  _             -> false
              end,
    case IsSynch of
        true ->
            {true, prepare_rpc_wait(R)};
        false ->
            false
    end.
%% check_if_synch(Opts) ->
%%     IsSynch = is_synch(Opts),
%%     case IsSynch of
%%      true ->
%%          {ReplyId, Opts1} = prepare_rpc_wait(Opts),
%%          ?debug("prepare_rpc_wait(~p) -> ~p",
%%                 [Opts, {ReplyId, Opts1}]),
%%          {true, ReplyId, Opts1};
%%      false ->
%%          {false, none, Opts}
%%     end.

%% is_synch(Opts) ->
%%     case lists:keyfind(<<"synch">>, 1, Opts) of
%%      {_, T} when T==true; T == <<"true">> ->
%%          true;
%%      false ->
%%          false
%%     end.

prepare_rpc_wait(R) ->
    NodeId = rvi_common:node_id(),
    Seq = integer_to_binary(erlang:unique_integer([positive, monotonic])),
    ReplyId = <<"$RVI/", NodeId/binary, "/", Seq/binary>>,
    push_route(R#{<<"reply_id">> => ReplyId}, ReplyId).

push_route(R, Id) ->
    PrevRoute = case R of
                    #{<<"route">> := Route} -> Route;
                    _ -> []
                end,
    R#{<<"route">> => [Id|R]}.

pop_route(Opts) ->
    ?debug("pop_route(~p)", [Opts]),
    case lists:keyfind(<<"route">>, 1, Opts) of
        false ->
            Opts;
        {_, [_|T1]} ->
            lists:keyreplace(<<"route">>, 1, Opts,
                             {<<"route">>, T1})
    end.

rpc_return(local, #{} = R, _, Res) ->
    case R of
        #{<<"synch">> := true} ->
            Res;
        _ ->
            ok
    end;
rpc_return(remote, #{} = R, Timeout, Res) ->
    case R of
        #{<<"reply_id">> := ReplyId} ->
            await_rpc_reply(ReplyId, Timeout);
        _ ->
            Res
    end.

await_rpc_reply(#{<<"reply_id">> := Tag, <<"timeout">> := Timeout}) ->
    %% FIXME: The Timeout is un-processed, and this is arguably structurally
    %% wrong.
    await_rpc_reply(Tag, Timeout).

await_rpc_reply(Tag, Timeout) ->
    receive
        {rvi, rpc_return, Tag, Reply} ->
            ?debug("received matching reply (Tag = ~p)~n~p", [Tag, Reply]),
            Reply;
        Other ->
            ?debug("Received Other = ~p", [Other]),
            {ok, internal}
    after Timeout ->
            {error, timeout}
    end.


do_register_service(#{<<"service_name">> := Svc,
                      <<"opts">> := Opts,
                      url = URL} = R) ->
    FullSvcName = rvi_common:local_service_to_string(Svc),
    Normalized = rvi_common:to_lower(FullSvcName),
    case service_handler:new(R#{full_service_name => FullSvcName,
                                normalized_name   => Normalized,
                		conn => local}) of
	{ok, R#{status => ok,
		service => FullSvcName}};
	{error, E} ->
	    {ok, internal}
    end.

register_local_service_(FullSvcName, URL, Opts, R, St) ->
    SvcOpts = parse_svc_opts(Opts),
    R1 = start_log("reg local service: ~s", [FullSvcName], R),
    ?debug("register_local_service(): full name: ~p ", [FullSvcName]),
    service_handler:new(FullSvcName, URL, SvcOpts),
    ets:insert(?SERVICE_TABLE, #service_entry {
				  key = to_lower(FullSvcName),
				  service = FullSvcName,
				  opts = SvcOpts,
				  url = URL }),

    %% Register with service discovery, will trigger callback to service_available()
    %% that forwards the registration to other connected services.
    service_discovery_rpc:register_services([FullSvcName], local),
    %% Return ok.
    {reply, R1#{status => ok,
		service => FullSvcName}, St}.


json_rpc_notification(Method, Parameters) ->
    jsx:encode(
      [{<<"json-rpc">>, <<"2.0">>},
       {<<"method">>, Method},
       {<<"params">>, Parameters}
      ]).

dispatch_to_local_service("ws:" ++ WSPidStr, services_available,
			  [{<<"services">>, Services}] ) ->
    ?info("service_edge:dispatch_to_local_service(service_available, websock, ~p): ~p",
	  [ WSPidStr,  Services]),
    wse_server:send(list_to_pid(WSPidStr),
		    json_rpc_notification(<<"services_available">>,
					  [{<<"services">>, Services}])),
    %% No reply
    ok;

dispatch_to_local_service("ws:" ++ WSPidStr, services_unavailable,
			  [{<<"services">>, Services}] ) ->
    ?info("service_edge:dispatch_to_local_service(service_unavailable, websock, ~p): ~p",
	  [ WSPidStr, Services]),

    wse_server:send(list_to_pid(WSPidStr),
		    json_rpc_notification(<<"services_unavailable">>,
					  [{<<"services">>, Services}])),
    ok;

%% dispatch_to_local_service([ $w, $s, $: | WSPidStr], message,
%% 			  [{service_name, SvcName}, {parameters, [Args]}]) ->
%%     ?info("service_edge:dispatch_to_local_service(message, websock): ~p", [Args]),
%%     wse_server:send(list_to_pid(WSPidStr),
%% 	     json_rpc_notification("message",
%% 				   [{ "service_name", SvcName}, {parameters, Args}])),
%%     %% No response expected.
%%     ?debug("service_edge:dispatch_to_local_service(message, websock): Done"),
%%     ok;

dispatch_to_local_service("ws:" ++ WSPidStr, message,
			 [{ <<"service_name">>, SvcName},
			  { <<"parameters">>, Args}]) ->
    ?info("service_edge:dispatch_to_local_service(message/alt, websock): ~p", [Args]),
    wse_server:send(list_to_pid(WSPidStr),
	     json_rpc_notification(<<"message">>,
				   [{<<"service_name">>, SvcName},
				    {<<"parameters">>, Args}])),
    %% No response expected.
    ?debug("service_edge:dispatch_to_local_service(message, websock): Done"),
    ok;

dispatch_to_local_service("ws:" ++ _WSPidStr, message, Other) ->
    ?warning("service_edge:dispatch_to_local_service(message/alt, websock): UNKNOWN: ~p", [Other]),
    ok;

%% Dispatch to regular JSON-RPC over HTTP.
dispatch_to_local_service(URL, Command, Args) ->
    CmdStr = atom_to_binary(Command, latin1),
    ?debug("dispatch_to_local_service():  Command:         ~p",[ Command]),
    ?debug("dispatch_to_local_service():  Args:            ~p",[ Args]),
    ?debug("dispatch_to_local_service():  URL:             ~p",[ URL]),
    Res = rvi_common:send_json_request(URL, CmdStr, Args),
    ?debug("dispatch_to_local_service():  Result:          ~p",[ Res]),
    Res.


%% Forward a message to a specific locally connected service.
%% Called by forward_message_to_local_service/2.
%%
forward_remote_msg_to_local_service(URL, #{<<"parameters">> := Params} = R) ->
    {Synch, Tag} =
	Synch0 = maps:get(<<"synch">>, R, false),
	case Synch0 of
	    T when T==true; T == <<"true">> ->
		case maps:find(<<"route">>, R) of
		    {ok, [Id|_]} ->
			{true, Id};
		    _ ->
			{false, none}
		end;
	    _ ->
		%% FIXME: this is not yet rewritten
		case rvi_common:get_json_element(
		       [<<"parameters">>,<<"synch">>], Params) of
		    {ok, T1} when T1 == true; T1 == <<"true">> ->
			case rvi_common:get_json_element(
			       [<<"parameters">>, <<"route">>], Params) of
			    {ok, [Id|_]} ->
				{true, Id};
			    _ ->
				{false, none}
			end;
		    _ ->
			{false, none}
		end
	end,
    forward_message_to_local_service(URL, R).

forward_message_to_local_service(URL, #{<<"service_name">> := SvcName} = R) ->
    %%
    %% Strip our node prefix from service_name so that
    %% the service receiving the JSON rpc call will have
    %% a service_name that is identical to the service name
    %% it registered with.
    %%
    LocalSvcName = strip_node_service_prefix(SvcName),
    ?debug("Service name: ~p", [LocalSvcName]),
    %% Deliver the message to the local service, which can
    %% be either a wse websocket, or a regular HTTP JSON-RPC call
    R1 = R#{<<"service_name">> => LocalSvcName},
    try
	log_outcome(
	  maybe_reply(
	    R1,
	    dispatch_to_local_service(URL, message, R1)),
	  R1)
    catch
	Tag:Err ->
	    ?error("Caught ~p:~p~n~p",
		   [Tag,Err,erlang:get_stacktrace()]),
	    {error, internal}
    end.

strip_node_service_prefix(SvcName) ->
    Pfx = rvi_common:local_service_prefix(),
    Sz = byte_size(Pfx)-1,
    <<_:Sz/binary, LocalSvcName0/binary>> = SvcName,
    normalize_slash(LocalSvcName0).


normalize_slash(Svc) ->
    {match, [Stripped]} = re:run(Svc, "/*(.*)", [{capture,[1],binary}]),
    <<"/", Stripped/binary>>.

maybe_reply(#{<<"synch">> := true}, Res) ->
    %% synch: decode and return result
    case decode_reply(Res) of
	{ok, Result} ->
	    Result;
	error ->
	    {error, internal}
    end;
maybe_reply(_, _) ->
    ok.
%% maybe_reply(#{<<"reply_id">> = ReplyId}, Res) ->
%%     ?debug("maybe_reply() Tag: ~p", [ReplyId]),
%%     ?debug("maybe_reply() Res: ~p", [Res]),
%%     log("schedule reply (~p)", [Tag], R),
%%     schedule_rpc:schedule_message(R, Tag, Opts,
%% 				  pop_route(Params))
%%     end,
%%     Res;
%% maybe_reply(true, Pid, Tag, Res, _Opts, _R) ->
%%     Pid ! {rvi, rpc_return, Tag, Res},
%%     Res.

decode_reply({_, Res}) when is_binary(Res) ->
    try jsx:decode(Res) of
	Decoded ->
	    {ok, Result} =
		rvi_common:get_json_element([<<"result">>], Decoded),
	    [{<<"result">>, Result}]
    catch
	error:_ ->
	    error
    end;
decode_reply({_, [{_,_}|_] = Res}) ->
    try rvi_common:get_json_element([<<"result">>], Res) of
	{ok, Result} ->
	    [{<<"result">>, Result}];
	_ ->
	    error
    catch
	error:_ ->
	    error
    end.

log_outcome({Status, _}, R) ->
    log("result: ~w", [Status], R);
log_outcome(Other, R) ->
    log("unexpected: ~w", [Other], R).



announce_service_availability(Available, SvcName) ->
    Cmd = case Available of
	      available -> services_available;
	      unavailable -> services_unavailable
	  end,

    %% See if we the service is already registered as a local
    %% service. If so, make sure that we don't send a service
    %% available to the URL tha originated the newly registered service.
    %%
    %% We also want to make sure that we don't send the notification
    %% to a local service more than once.
    %% We will build up a list of blocked URLs not to resend to
    %% as we go along
    BlockURLs = case lookup_service(SvcName)  of
		    [#service_entry{url = URL}]  -> [URL];
		    [] -> []
		end,
    ets:foldl(fun(Term, _Acc) ->
		      ?debug("~p: ~p~n", [?SERVICE_TABLE, Term]),
		      ok
	      end, ok, ?SERVICE_TABLE),
    ?debug("announce: service: ~p", [SvcName]),
    ?debug("announce: Block:   ~p", [BlockURLs]),

    ets:foldl(
      %% Notify if this is not the originating service.
      fun(#service_entry{url = URL}, Acc) ->
	      %% If the URL is not on the blackout
	      %% list, send a notification
	      case lists:member(URL, Acc) of
		  false ->
		      ?debug("DISPATCH: ~p: ~p", [URL, Cmd]),
		      dispatch_to_local_service(URL, Cmd, [{<<"services">>,
							    [SvcName]}]),
		      %% Add the current URL to the blackout list
		      [URL | Acc];

		  %% URL is on blackout list
		  true ->
		      Acc
	      end
      end, BlockURLs, ?SERVICE_TABLE).

start_log(Fmt, Args, #{<<"log_id">> := LogID} = R) ->
    log(Fmt, Args, R);
start_log(Fmt, Args, #{} = R) ->
    start_log_(rvi_log:new_id(pfx()), Fmt, Args, R).

start_log_(ID, Fmt, Args, #{} = R) ->
    R1 = R#{<<"log_id">> => ID},
    log(Fmt, Args, R1),
    R1.

pfx() ->
    <<"svc_edge">>.

log(Fmt, Args, #{} = R) ->
    rvi_log:flog(Fmt, Args, pfx(), R).

log_id_json_tail(Args) ->
    ?debug("Args = ~p", [Args]),
    case rvi_common:get_json_element([<<"rvi_log_id">>], Args) of
	{ok, ID} ->
	    [{<<"rvi_log_id">>, ID}];
	{error, _} ->
	    []
    end.

event(_, _, _) ->
    ok.

%% append_files_to_params([], Parameters) ->
%%     Parameters;
%% append_files_to_params(Files, [T|_] = Parameters) when is_tuple(T) -> % object
%%     Parameters ++ [{<<"files">>, Files}];
%% append_files_to_params(Files, [_|_] = Parameters) ->  % array
%%     Parameters ++ [[{<<"files">>, Files}]].


parse_svc_opts(Opts) ->
    Files = rvi_common:get_opt_json_element(<<"files">>, <<"inline">>, Opts),
    [{<<"files">>, files_option(Files)}].

files_option(O = <<"inline">>)    -> O;
files_option(O = <<"reject">>)    -> O;
files_option(O = <<"multipart">>) -> O;
files_option(_) ->
    throw(invalid_command).

msg_options(Params) ->
    [{K, V} || {K, V} <- Params,
	       lists:member(K, [<<"route">>,
				<<"timeout">>,
				<<"synch">>,
				<<"files">>])].

timeout_arg(TimeoutArg) ->
    %%
    %% Slick but ugly.
    %% If the timeout is more than 24 hrs old when parsed as unix time,
    %% then we are looking at a relative msec timeout. Convert accordingly
    %%
    {Mega, Sec, _Micro} = os:timestamp(),
    Now = Mega * 1000000 + Sec,
    case TimeoutArg - Now < -86400 of
	true -> %% Relative timeout arg. Convert to unix time msec
	    ?debug("service_edge_rpc:local_msg(): Timeout ~p is relative.",
		   [TimeoutArg]),
	    (Now * 1000) + TimeoutArg;
	false -> %% Absolute timeout. Convert to unix time msec
	    TimeoutArg * 1000
    end.

timeout_ms(UnixT) ->
    {M,S,U} = os:timestamp(),
    Now = M * 1000000000 + (S * 1000) + (U div 1000),
    UnixT - Now.

validate(Msg, Args) ->
    dlink_msg:validate(service_edge, Msg, Args).

to_lower(Svc) when is_binary(Svc) ->
    rvi_common:to_lower(Svc).
