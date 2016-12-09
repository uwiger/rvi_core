%%
%% Copyright (C) 2014, Jaguar Land Rover
%%
%% This program is licensed under the terms and conditions of the
%% Mozilla Public License, version 2.0.  The full text of the
%% Mozilla Public License is at https://www.mozilla.org/MPL/2.0/
%%

-module(schedule_rpc).
-behaviour(gen_server).

-include_lib("lager/include/log.hrl").
-include_lib("rvi_common/include/rvi_common.hrl").
%% API
-export([start_link/0]).
-export([schedule_message/1]).

%% Invoked by service discovery
%% FIXME: Should be rvi_service_discovery behavior
-export([service_available/2,
	 service_unavailable/2]).
-export([publish_node_id/3]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
	 terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

%% Message structure and storage
%%  A message is a piece of
%%
%%  Service -> ETS -> Messages
-record(service, {
	  key = {"", unknown}, %% Service name (lowercase) and data link module
	  service = "",  %% real service name

	  %% Is this service currently available on the data link module
	  available = false,

	  %% Table containing #message records,
	  %% indexed by their transaction ID (and sequence of delivery)
	  messages_tid =  undefined
	 }).


% A single message to be delivered to a service.
% Messages are stored in ets tables hosted by a service

-record(entry, {
	  transaction_id, %% Transaction ID that message is tagged with.
	  service,        %% Target service
	  timeout,        %% Timeout, UTC
	  data_link,      %% Data Link Module to use. {Module, Opts}
	  message,
	  routes,         %% Routes retrieved for this
	  timeout_tref,   %% Ref to erlang timer associated with this message.
	  log_id
	 }).

-record(st, {
	  next_transaction_id = 1, %% Sequentially incremented transaction id.
	  services_tid = undefined,
	  cs %% Service specification
	 }).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, St} |
%%                     {ok, St, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
init([]) ->
    %% Setup the relevant ets tables.
    %% Unsubscribe from service availablility notifications
    CS = rvi_common:get_component_specification(),
    service_discovery_rpc:subscribe(CS, ?MODULE),
    {ok, #st{
	    cs = CS,
	    services_tid = ets:new(rvi_schedule_services,
				   [set, private, {keypos, #service.key}])}}.

schedule_message(#{} = Msg) ->
    gen_server:call(?MODULE, {schedule_message, Msg}).

publish_node_id(NodeId, DLMod) ->
    P = self(),
    S = <<"$RVI/", NodeId/binary>>,
    spawn(fun() ->
                  MRef = erlang:monitor(process, P),
                  receive
                      {'DOWN', MRef, _, _, _} ->
                          service_unavailable(S, DLMod)
                  end
          end),
    service_available(S, DLMod).

service_available(SvcName, DataLinkModule) ->
    cast({service_available, SvcName, DataLinkModule}).

service_unavailable(SvcName, DataLinkModule) ->
    cast({service_unavailable, SvcName, DataLinkModule}).

cast(Msg) ->
    gen_server:cast(?MODULE, Msg).

handle_call({schedule_message,
	     #{<<"service">> := SvcName,
	       normalized_name := Normalized} = Msg}, {Pid, _Ref}, St) ->
    ?debug("sched_msg(): service:     ~p", [SvcName]),

    %% Create a transaction ID
    {TransID, St1} = create_transaction_id(St),
    log(Msg, "queue: tid=~w", [TransID]),
    %% Queue the message
    {_, St2} = queue_message(
		 Msg#{pid => Pid}, rvi_routing:get_service_routes(Normalized),
		 St1),
    {reply, {ok, TransID}, St2};
handle_call(Other, _From, St) ->
    ?warning("sched:handle_call(~p): unknown", [Other]),
    {reply,  {error, unknown_command}, St}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @spec handle_cast(Msg, St) -> {noreply, St} |
%%                                  {noreply, St, Timeout} |
%%                                  {stop, Reason, St}
%% @end
%%--------------------------------------------------------------------

handle_cast({service_available, SvcName, DataLinkModule}, St) ->
    %% Find or create the service.
    ?debug("service_available(): ~p:~s", [DataLinkModule, SvcName]),
    %% Create a new or update an existing service.
    SvcRec = update_service(SvcName, DataLinkModule, available, St),

    %% Try to send any pending messages waiting for this
    %% service / data link combo.
    {_, NSt1} = send_queued_messages(SvcRec, St),

    %% Send any orphaned messages waiting for the service
    %% to come up
    NSt2 = send_orphaned_messages(SvcName, DataLinkModule, NSt1),
    {noreply, NSt2};

handle_cast({service_unavailable, SvcName, DataLinkModule},
	    #st{services_tid = SvcTid} = St) ->
    %% Grab the service
    case ets:lookup(SvcTid, {to_lower(SvcName), DataLinkModule}) of
	[] ->  %% No service found - no op.
	    {noreply, St};
	[SvcRec] ->
	    %% Delete service if it does not have any pending messages.
	    case delete_unused_service(SvcTid, SvcRec) of
		true ->  %% service was deleted
		    {noreply, St};
		false -> %% SvcName was not deleted, set it to not available
		    update_service(SvcName, DataLinkModule, unavailable, St),
		    {noreply, St}
	    end
    end;

handle_cast(Other, St) ->
    ?warning("sched:handle_cast(~p): unknown", [ Other ]),
    {noreply, St}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, St) -> {noreply, St} |
%%                                   {noreply, St, Timeout} |
%%                                   {stop, Reason, St}
%% @end
%%--------------------------------------------------------------------

%% Handle timeouts
handle_info({rvi_message_timeout, SvcName, DLMod, TransID},
	    #st{services_tid = SvcTid} = St) ->
    ?debug("timeout(~p:~p): trans_id: ~p", [DLMod, SvcName, TransID]),
    %% Look up the service / DataLink mod
    case ets:lookup(SvcTid, {to_lower(SvcName), DLMod}) of
	[SvcRec] -> %% Found service for specific data link
	    %% Delete message from service queue
	    case ets:lookup(SvcRec#service.messages_tid, TransID) of
		[Msg] ->
		    ?debug("timeout(~p:~p): Rescheduling", [DLMod, SvcName]),
		    ets:delete(SvcRec#service.messages_tid, TransID),
		    %% Calculate
		    TOut = calc_relative_tout(Msg),
		    %% Has the message itself, not only the current
		    %% data link attempt, timed out?
		    case TOut =:= -1 of
			true ->
			    %% Yes!
			    tell_pid({rvi, schedule, message_timeout}, Msg),
			    {noreply, St};
			false ->
			    %% Try to requeue message
			    {_Res, NSt} =
				queue_message(Msg,
					      Msg#entry.routes,
					      St),
			    {noreply, NSt}
		    end;
		_ ->
		    ?debug("timeout(): trans_id(~p) service(~p): "
			   "Yanked while processing",
			  [TransID, SvcName]),
		    {noreply, St}
	    end;
	_ ->
	    ?debug("timeout(~p:~p): Unknown service", [DLMod, SvcName]),
	    {noreply, St}
    end;

handle_info(Info, St) ->
    ?debug("handle_info(): Unknown: ~p", [Info]),
    {noreply, St}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, St) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, _St) ->
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process st when code is changed
%%
%% @spec code_change(OldVsn, St, Extra) -> {ok, NewSt}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, St, _Extra) ->
    {ok, St}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

tell_pid(Msg, #{pid := Pid}) when is_pid(Pid) ->
    Pid ! Msg;
tell_pid(_, _) ->
    ignored.


store_message(SvcRec, DataLinkMod, Message, RelativeTimeout) ->
    {SvcName, _} = SvcRec#service.key,
    TRef = erlang:send_after(RelativeTimeout, self(),
			     {rvi_message_timeout,
			      SvcName,
			      DataLinkMod,
			      Message#entry.transaction_id}),
    %% Add message to the service's queue, with an updated
    %% timeout ref.
    ets:insert(SvcRec#service.messages_tid,
	       Message#entry{timeout_tref = TRef}),
    ok.

%%
%% No more routes to try, or no routes found at all
%% by rvi_routing:get_service_routes()
%%
%% Stash the message in the unknown datalinvariant of the service
%% and opportunistically send it if the service
queue_message(#{<<"service">> := SvcName,
		normalized_name = Normalized,
		<<"timeout">> := Timeout} = Msg, [], St) ->

    TOut = calc_relative_tout(Timeout),
    ?debug("sched:q(~s): No more routes. Will orphan for ~p seconds.",
	   [SvcName, TOut / 1000.0]),
    %% Stash in Service / orphaned
    SvcRec = find_or_create_service(Normalized, orphaned, St),
    store_message(SvcRec,
		  orphaned,
		  #entry{
		     routes =  [],
		     message = Msg,
		     timeout_tref = 0
		   },
		  TOut),
    {ok, St};

%% Try to queue message
queue_message(#entry{service = SvcName, timeout = Timeout} = Msg,
	      [{{ProtoMod, ProtoOpt}, {DLMod, DLOpt}} | RemainingRoutes],
	      St) ->
    ?debug("sched:q(~p:~s): timeout:      ~p", [DLMod, SvcName, Timeout]),
    #service{key = {Service,_}} = SvcRec =
	find_or_create_service(SvcName, DLMod, St),
    %%
    %% Bring up the relevant data link for the given route.
    %% Once up, the data link will invoke service_availble()
    %% to indicate that the service is available for the given DL.
    %%
    Msg1 = Msg#entry{
	     data_link = {DLMod, DLOpt},
	     routes = RemainingRoutes,
	     timeout_tref = 0
	    },
    case DLMod:setup_data_link(St#st.cs, Service, DLOpt) of
	{ok, DLTimeout} ->
	    TOut = select_timeout(calc_relative_tout(Timeout), DLTimeout),
	    ?debug("sched:q(~p:~s): ~p seconds to compe up.",
		   [ DLMod, SvcName, TOut / 1000.0]),

	    store_message(SvcRec, DLMod, Msg1, TOut),
	    {ok, St};

	[ already_connected, _] ->
	    %% The service may already be available, give it a shot.
	    ?debug("sched:q(~p:~s): already up. Sending.",
		   [ DLMod, SvcName]),

	    %% Will re-queue message if cannot send.
	    { _, NSt } =
		send_message(DLMod, DLOpt, ProtoMod, ProtoOpt, Msg1, St),
	    { ok, NSt };


	%%
	%% We failed to setup a data link. Try the next route.
	%%
	[ Err, _Reason] ->
	    ?debug("sched:q(~p:~s): failed to setup: ~p", [ DLMod, SvcName, Err]),
	    queue_message(Msg1, RemainingRoutes, St)
    end.

%% Send messages to locally connected service
send_message(local, _,  _, _,   Msg, St) ->

    ?debug("sched:send_msg(local:~s). WIll send to local",
	   [ Msg#entry.service]),

    service_edge_rpc:handle_remote_message(St#st.cs,
					   Msg#entry.service,
					   Msg#entry.options,
					   Msg#entry.parameters),
    {ok, St};

%% Forward message to protocol.
send_message(DataLinkMod, DataLinkOpts,
	     ProtoMod, ProtoOpts,
	     Msg, St) ->
    ?debug("sched:send_msg(): ~p:~p:~p",
	   [ProtoMod, DataLinkMod, Msg#entry.service]),

    %% Send off message to the correct protocol module
    case ProtoMod:send_message(
	   St#st.cs,
	   Msg#entry.transaction_id,
	   Msg#entry.service,
	   Msg#entry.options,
	   ProtoOpts,
	   DataLinkMod,
	   DataLinkOpts,
	   Msg#entry.parameters) of

	%% Success
	[ok] ->
	    %% Send the rest of the messages associated with this service/dl
	    {ok, St};

	%% Failed
	[Err] ->
	    ?info("sched:send_msg(): Failed: ~p:~p:~p -> ~p",
		  [ProtoMod, DataLinkMod, Msg#entry.service, Err]),

	    %% Requeue this message with the next route
	    queue_message(Msg, Msg#entry.routes, St)
    end.


%% The service is not available
send_queued_messages(#service {
		      key = { SvcName, _ },
		      available = unavailable,
		      messages_tid = _Tid } = _SvcRec, St) ->

    ?info("sched:send(): SvcName:   ~p: Not available", [SvcName]),
    { not_available, St };

send_queued_messages(#service {
			key = { SvcName, DataLinkMod },
			available = available,
			messages_tid = Tid } = SvcRec, St) ->

    ?debug("sched:send(): Service:    ~p:~s", [DataLinkMod, SvcName]),

    %% Extract the first message of the queue.
    case first_service_message(SvcRec) of
	empty ->
	    ?debug("sched:send(): Nothing to send"),
	    { ok, St };

	yanked ->
	    ?info("sched:send(): Message was yanked while trying to send: ~p",
		  [SvcRec#service.key]),
	    { ok, St};

	Msg->
	    %% Wipe from ets table and cancel timer
	    ets:delete(Tid, Msg#entry.transaction_id),
	    erlang:cancel_timer(Msg#entry.timeout_tref),
	    %% Extract the protocol / data link to use
	    { DataLink, DataLinkOpts } = Msg#entry.data_link,
	    { Proto, ProtoOpts } = Msg#entry.protocol,
	    { _, NSt} = send_message(DataLink, DataLinkOpts,
				     Proto, ProtoOpts,
				     Msg, St),

	    send_queued_messages(SvcRec, NSt)
    end.


%% Check in the orphaned queue for our locally connected service,
%% where messages are placed while waiting for a final timeout after
%% all routes have failed.
%%
%% If messages exist in the orphaned queue for SvcName,
%% we will try to send them using the DataLink/Protocol combo
%% provided on the command line.
send_orphaned_messages(SvcName, local, St) ->

    %% See if there is an orphaned queue for SvcName
    case find_service(SvcName, orphaned, St) of
	 not_found ->
	    ?debug("sched:send_orph(~p:~p): No orphaned messages waiting",
		   [ local,SvcName]),
	    St;

	%% We have orphaned messages for the service and
	%% we have at least one protocol that we can use over
	%% the given data link
	%% Start chugging out messages
	SvcRec ->
	    send_orphaned_messages_(undefined, undefined,
				    local, undefined,
				    SvcRec, St)
   end;

%% Check in the orphaned queue for the given service, where messages
%% are placed while waiting for a final timeout after all routes
%% have failed.
%%
%% If messages exist in the orphaned queue for SvcName,
%% we will try to send them using the DataLink/Protocol combo
%% provided on the command line.
send_orphaned_messages(SvcName, DataLinkMod, St) ->

    %% See if there is an orphaned queue for SvcName
    case find_service(SvcName, orphaned, St) of
	not_found ->
	    St;
	SvcRec ->
	    send_orphaned_messages(SvcName, SvcRec, DataLinkMod, St)
    end.

send_orphaned_messages(SvcName, SvcRec, DataLinkMod, St) ->
    case rvi_routing:get_service_protocols(SvcName, DataLinkMod) of
	%% We have messages waiting for the service, but no protocol has been
	%% configured for transmitting them over the given data link module
	[] ->
	    ?debug("sched:send_orph(~p:~p): No protocol configured. Skipped",
		   [DataLinkMod, SvcName]),
	    St;

	%% We have orphaned messages for the service and
	%% we have at least one protocol that we can use over
	%% the given data link
	%% Start chugging out messages
	[{ Proto, ProtoOpts, DataLinkOpts }  | _] ->
	    send_orphaned_messages_(Proto, ProtoOpts,
				    DataLinkMod, DataLinkOpts,
				    SvcRec, St)
    end.

send_orphaned_messages_(Protocol, ProtocolOpts,
			DataLinkMod, DataLinkOpts,
			#service {
			   key = { SvcName, _ },
			   messages_tid = Tid } = SvcRec, St) ->


    %% Extract the first message of the queue.
    case first_service_message(SvcRec) of
	empty ->
	    ?debug("sched:send_orph(~p:~p): Nothing to send",
		   [DataLinkMod, SvcName ]),
	    St;

	yanked ->
	    ?info("sched:send_orph(~p:~p): Message was yanked while processing",
		   [DataLinkMod, SvcName ]),
	    send_orphaned_messages_(DataLinkMod, DataLinkOpts,
				    Protocol, ProtocolOpts,
				    SvcRec, St);

	Msg->
	    %% Wipe from ets table and cancel timer
	    ets:delete(Tid, Msg#entry.transaction_id),
	    erlang:cancel_timer(Msg#entry.timeout_tref),

	    ?debug("sched:send_orph(~p:~p): Sending Trans(~p) Pr(~p) PrOp(~p)  DlOp(~p)",
		   [DataLinkMod, SvcName,
		    Msg#entry.transaction_id,
		    Protocol, ProtocolOpts, DataLinkOpts]),

	    { _, NSt} = send_message( DataLinkMod, DataLinkOpts,
				      Protocol, ProtocolOpts,
				      Msg, St),

	    send_orphaned_messages_(DataLinkMod, DataLinkOpts,
				    Protocol, ProtocolOpts,
				    SvcRec, NSt)
    end.


find_service(<<"$rvi/", Rest/binary>>, DLMod, #st{services_tid = SvcTid}) ->
    case re:split(Rest, <<"/">>, [{return, binary}]) of
	[NodeId | _] ->
	    ?debug("NodeId = ~p", [NodeId]),
	    case ets:lookup(SvcTid, {<<"$rvi/", NodeId/binary>>, DLMod}) of
		[] ->
		    not_found;
		[SvcRec] ->
		    SvcRec
	    end;
	_ ->
	    not_found
    end;
find_service(SvcName, DataLinkMod, #st{services_tid = SvcTid}) ->
    ?debug("sched:find_or_create_service(): ~p:~p", [ DataLinkMod, SvcName]),
    case ets:lookup(SvcTid, {SvcName, DataLinkMod }) of
	[] ->  %% The given service does not exist, return not found
	    not_found;
	[SvcRec] ->
	    SvcRec
    end.

find_or_create_service(SvcName, DataLinkMod, St) ->
    ?debug("sched:find_or_create_service(): ~p:~p", [DataLinkMod, SvcName]),
    case find_service(SvcName, DataLinkMod, St) of
	not_found ->  %% The given service does not exist, create it.
	    ?debug("find_or_create_service(): Creating new ~p", [ SvcName]),
	    update_service(SvcName, DataLinkMod, unavailable, St);
	SvcRec ->
	    %% Update the network address, if it differs, and return
	    %% the new service / State as {ok, NSvcRec, false, NSt}
	    ?debug("find_or_create_service(): Updating existing ~p", [SvcName]),
	    SvcRec
    end.

%% Create a new service.
%% Warning: Will overwrite existing service (and its message table reference).
%%
update_service(SvcName, DataLinkMod, Available,
	       #st{services_tid = SvcsTid, cs = CS}) ->
    SvcKey = rvi_common:to_lower(SvcName),
    Key = {SvcKey, DataLinkMod},
    MsgTID  =
	case ets:lookup(SvcsTid, Key) of
	    [] ->  %% The given service does not exist, create a new message TID
		?debug("update_service(~p:~p): ~p - Creating new",
		       [DataLinkMod, SvcName, Available]),
		ets:new(rvi_messages,
			[ordered_set, private,
			 {keypos, #entry.transaction_id}]);
	    [TmpSvcRec] ->
		%% Grab the existing message table ID
		?debug("update_service(~p:~p): ~p - Updating existing",
		       [ DataLinkMod, SvcName, Available]),
		#service{messages_tid = TID} = TmpSvcRec,
		TID
	end,
    %% Insert new service to ets table.
    SvcRec = #service{
		key = {SvcKey, DataLinkMod},
		service = SvcName,
		available = Available,
		messages_tid = MsgTID,
		cs = CS
	       },
    ets:insert(SvcsTid, SvcRec),
    SvcRec.

%% Create a new and unique transaction id
create_transaction_id(St) ->
    ?debug("sched:create_transaction_id(~p): ", [  St#st.next_transaction_id ]),
    ID = St#st.next_transaction_id,

    %% FIXME: Maybe integrate pid into transaction to handle multiple
    %% schedulers?
    {ID, St#st{next_transaction_id = ID + 1}}.

%% Calculate a relative timeout based on the Msec UnixTime TS we are
%% provided with.
calc_relative_tout(#{<<"timeout">> := UnixTimeMS}) ->
    Now = erlang:system_time(milli_seconds),
    ?debug("sched:calc_relative_tout(): TimeoutUnixMS(~p) - Now(~p) = ~p",
	   [UnixTimeMS, Now, UnixTimeMS - Now]),
    %% Cap the timeout value at something reasonable
    TOut = UnixTimeMS - Now,
    case TOut =< 0 of
	true ->
	    -1; %% We have timed out
	false ->
	    TOut
    end.
%% Handle a callback for a timed out message.

do_timeout_callback(CompSpec, SvcName, TransID) ->
    service_edge_rpc:handle_local_timeout(CompSpec, SvcName, TransID),
    ok.

%% Kill off a service that is no longer used.
%%
delete_unused_service(SvcTid, SvcRec) ->
    %% Do we have messages waiting for this service?
    case ets:first(SvcRec#service.messages_tid) of
	%% Nope.
	'$end_of_table' ->
	    ets:delete(SvcRec#service.messages_tid),
	    ets:delete(SvcTid, SvcRec#service.key),

	    %% Update the network address, if it differs, and return
	    %% the new service / State as {ok, NSvcRec, false, NSt}
	    ?debug("service_unavailable(): Service ~p now has no address.",
		   [SvcRec#service.key]),
	    true;
	_ ->
	    false
    end.

first_service_message(#service{messages_tid = Tid}) ->
    case ets:first(Tid) of
	'$end_of_table' ->
	    empty;
	Key ->
	    case ets:lookup(Tid, Key) of
		[Msg] -> Msg;
		[]    -> yanked
	    end
    end.

%% A timeout of -1 means 'does not apply'
%%
%% However, if both are -1, we just return the shortest posssible
%% timeout.
%% If both timeouts are specified, we select the shortest one.
%%
select_timeout(TimeOut1, TimeOut2) ->
    case { TimeOut1 =:= -1, TimeOut2 =:= -1 } of
	{true, true}	-> 1;
	{true, false}	-> TimeOut2;
	{false, true}	-> TimeOut1;
	{false, false}	-> min(TimeOut1, TimeOut2)
    end.

log(#{} = R, Fmt, Args) ->
    rvi_log:log(R, <<"schedule">>, rvi_log:format(Fmt, Args));
log(_, _, _) ->
    ok.

to_lower(Str) ->
    rvi_common:to_lower(Str).
