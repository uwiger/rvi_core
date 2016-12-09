%%
%% Copyright (C) 2014, Jaguar Land Rover
%%
%% This program is licensed under the terms and conditions of the
%% Mozilla Public License, version 2.0.  The full text of the
%% Mozilla Public License is at https://www.mozilla.org/MPL/2.0/
%%


-module(service_discovery_rpc).
-behaviour(gen_server).

-export([start_link/0]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
	 terminate/2, code_change/3]).

-export([get_all_services/0,
	 get_services_by_module/1,
	 get_modules_by_service/1,
	 is_service_available/1,
	 subscribe/1,
	 unsubscribe/1,
	 register_services/2,
	 unregister_services/2]).

-include_lib("lager/include/log.hrl").
-include_lib("rvi_common/include/rvi_common.hrl").

-define(SERVICE_TABLE, rvi_svcdisc_services).
-define(MODULE_TABLE, rvi_svcdisc_modules).
-define(SUBSCRIBER_TABLE, rvi_svcdisc_subscribers).

-record(service_entry, {
	  key = [],
	  service = [],             %% Service handled by this entry.
	  data_link_mod = undefined %% Module handling service, 'local' if local service
	 }).


-record(subscriber_entry, {
	  module %% Module subscribing to service availability
	 }).


-define(SERVER, ?MODULE).

-record(st, {
	  %% Component specification
	  cs = #component_spec{}
	 }).

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

init([]) ->
    ?info("svc_disc:init(): called."),
    ets:new(?SERVICE_TABLE, [duplicate_bag, public, named_table,
			     {keypos, #service_entry.key}]),

    ets:new(?MODULE_TABLE, [duplicate_bag,  public, named_table,
			    {keypos, #service_entry.data_link_mod}]),

    ets:new(?SUBSCRIBER_TABLE, [set, public, named_table,
				  {keypos, #subscriber_entry.module}]),

    {ok, #st{cs = rvi_common:get_component_specification()}}.


get_all_services() ->
    call(get_all_services).

get_services_by_module(DataLinkMod) ->
    try to_existing_atom(DataLinkMod) of
	M ->
	    call({get_services_by_module, M})
    catch
	error:_ ->
	    []
    end.

get_modules_by_service(Service) ->
    call({get_modules_by_service, Service}).

is_service_available(Service) ->
    call({is_service_available, Service}).

register_services([], _) ->
    ok;
register_services(Services, DataLinkModule) ->
    ?debug("register_services(Mod=~p): ~p", [DataLinkModule, Services]),
    cast({register_services, Services, DataLinkModule}).

unregister_services([], _) ->
    ok;
unregister_services(Services, DataLinkModule) ->
    ?debug("unregister_services(Mod=~p): ~p", [DataLinkModule, Services]),
    cast({unregister_services, Services, DataLinkModule}).

subscribe(SubscribingMod) ->
    cast({subscribe, SubscribingMod}).

subscribe_connection(Conn, Mod) ->
    %% FIXME!! Subscribe a connection to service announcements,
    %% where service_discovery filters the list based on stored credentials
    Svcs = authorize_keys:authorized_services_for_conn(Conn),
    Mod:services_available(Svcs),
    gproc:reg({p, l, {?MODULE, subscr_conn, Conn, Mod)}),
    ok.


unsubscribe(SubscribingMod) ->
    cast({unsubscribe, SubscribingMod}).

call(Req) ->
    case gen_server:call(?MODULE, Req) of
	{error, internal_error} ->
	    error(internal_error)
    end.

cast(Msg) ->
    gen_server:cast(?MODULE, Msg).

to_existing_atom(B) when is_binary(B) ->
    binary_to_existing_atom(B, latin1);
to_existing_atom(L) when is_list(L) ->
    list_to_existing_atom(L);
to_existing_atom(A) when is_atom(A) ->
    A.

handle_call(Req, From, St) ->
    try handle_call_(Req, From, St)
    catch
	error:Reason ->
	    ?debug("~p:handle_call_(~p,~p,~p) -> ERROR: ~p~n~p",
		   [?MODULE, Req, From, St, Reason, erlang:get_stacktrace()]),
	    {reply, {error, internal_error}, St}
    end.

handle_call_(get_all_services, _From, St) ->
    Svcs = ets:foldl(fun(#service_entry {service = ServiceName}, Acc) ->
			    [ServiceName | Acc] end,
		    [], ?SERVICE_TABLE),
    {reply,  Svcs, St};
handle_call_({get_services_by_module, Module}, _From, St) ->
    {reply,  get_services_by_module_(Module), St};
handle_call_({get_modules_by_service, Service}, _From, St) ->
    {reply,  get_modules_by_service_(Service), St};
handle_call_({is_service_available, Service}, _From, St) ->
    {reply, ets:member(?SERVICE_TABLE, to_lower(Service)), St};
handle_call_(Other, _From, St) ->
    ?warning("svc_disc:handle_call(~p): unknown", [ Other ]),
    {reply,  {error, unknown_command} , St}.

handle_cast({subscribe, SubsMod}, St) ->
    %% Insert new entry, or replace existing one
    ets:insert(?SUBSCRIBER_TABLE, #subscriber_entry{module = SubsMod}),
    initial_notification(SubsMod),
    {noreply, St};

handle_cast({unsubscribe, SubsMod}, St) ->
    ets:delete(?SUBSCRIBER_TABLE, SubsMod),
    {noreply, St};
handle_cast({register_services, Services, DLModule}, St) ->
    ?info("register_services(): ~p:~p", [DLModule, Services]),
    [register_single_service_(SvcName, DLModule) || SvcName <- Services],
    notify_subscribers(available, Services, DLModule),
    {noreply, St};
handle_cast({unregister_services, Services, DLModule}, St) ->
    ?info("unregister_services(): ~p:~p", [DLModule, Services]),
    [unregister_single_service_(SvcName, DLModule) || SvcName <- Services],
    notify_subscribers(unavailable, Services, DLModule),
    {noreply, St};
handle_cast(Other, St) ->
    ?warning("svc_disc:handle_cast(~p): unknown", [ Other ]),
    {noreply, St}.

handle_info(_Info, St) ->
    {noreply, St}.

terminate(_Reason, _St) ->
    ok.
code_change(_OldVsn, St, _Extra) ->
    {ok, St}.

register_single_service_(Service, DataLinkModule) ->
    %% Delete any previous instances of the given entry, in case
    %% the service registers multiple times
    SvcKey = to_lower(Service),
    unregister_single_service_(Service, DataLinkModule),
    ets:insert(?SERVICE_TABLE,
	       #service_entry {
		  key = SvcKey,
		  service = Service,
		  data_link_mod = DataLinkModule, _ = '_'
		 }),
    ets:insert(?MODULE_TABLE,
	       #service_entry {
		  key = SvcKey,
		  service = Service,
		  data_link_mod = DataLinkModule, _ = '_'
		 }),
    ok.

unregister_single_service_(Service, DataLinkModule) ->
    %% Delete any service table entries with a matching Service.
    SvcKey = to_lower(Service),
    ets:match_delete(?SERVICE_TABLE,
		     #service_entry {
			key = SvcKey,
			data_link_mod = DataLinkModule, _ = '_'
		      }),
    %% Delete any remote address table entries with a matching Service.
    ets:match_delete(?MODULE_TABLE,
		     #service_entry {
			key = SvcKey,
			data_link_mod = DataLinkModule, _ = '_'
		      }),
    ok.

%%
%% Return all modules that can currently route the provided service.
%%
get_modules_by_service_(Service) ->
    ModNames = [Mod || #service_entry{data_link_mod = Mod}
			   <- ets:lookup(?SERVICE_TABLE, to_lower(Service))],
    ?debug("get_modules_by_service_(): ~p -> ~p", [Service, ModNames]),
    ModNames.

get_services_by_module_(Module) ->
    SvcNames = [Svc || #service_entry{service = Svc}
			   <- ets:lookup(?MODULE_TABLE, Module)],
    ?debug("get_services_by_module_(): ~p -> ~p", [Module, SvcNames]),
    SvcNames.

notify_subscribers(Available, Services, DataLinkModule) ->
    ?debug("notify_subscribers(~p:~p) ~p",
	   [DataLinkModule, Services, Available]),
    %% Figure out the function to invoke
    Fun = case Available of
	      available -> service_available;
	      unavailable -> service_unavailable
	  end,
    ets:foldl(
      %% Notify if this is not the originating service.
      fun(#subscriber_entry{module = Module}, _Acc) ->
	      ?debug("  notify_subscribers module: ~p ", [Module]),
	      _ = [Module:Fun(Svc, DataLinkModule) || Svc <- Services],
	      ok
      end, ok, ?SUBSCRIBER_TABLE).

initial_notification(SubsMod) ->
    ets:foldl(
      fun(#service_entry{service = S, data_link_mod = DataLinkMod}, Acc) ->
	      SubsMod:service_available(S, DataLinkMod)
      end, ok, ?SERVICE_TABLE).

to_lower(Svc) ->
    rvi_common:to_lower(Svc).
