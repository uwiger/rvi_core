%% -*- erlang-indent-level: 4; indent-tabs-mode: nil -*-
%%
%% Copyright (C) 2016, Jaguar Land Rover
%%
%% This program is licensed under the terms and conditions of the
%% Mozilla Public License, version 2.0.  The full text of the
%% Mozilla Public License is at https://www.mozilla.org/MPL/2.0/
%%

-module(dlink_gen_rpc).
-behavior(gen_server).

-export([start_link/3,
         init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-export([service_available/3,
         service_unavailable/3]).

-include_lib("lager/include/log.hrl").
-include_lib("rvi_common/include/rvi_common.hrl").
-include_lib("rvi_common/include/rvi_dlink_bin.hrl").

-define(PERSISTENT_CONNECTIONS, persistent_connections).
-define(SERVER_OPTS, server_opts).
-define(DEFAULT_RECONNECT_INTERVAL, 5000).
-define(DEFAULT_ADDRESS, "0.0.0.0").

-define(DEFAULT_ACCEPTORS, 10).

-record(service_entry, {
          service = [],
          connections = []}).

-record(connection_entry, {
          connection = undefined :: pid() | undefined,
          services = []
         }).

-record(listener, {
          socket,
          options = [],
          acceptors = []}).

-record(pconn, {
          pid,
          mref,
          ip,
          port,
          opts}).


-record(st, {
          mod,
          mod_st,
          my_ip,
          my_port,
          listeners = [],
          persistent_connections = []
        }).

start_link(Name, Module, Arg) ->
    gen_server:start_link({local, Name}, ?MODULE, {Module, Arg}, []).

service_available(Svc, DLMod, Mod) ->
    tell_conns(Mod, {service_available, Svc, DLMod}).

service_unavailable(Svc, DLMod, Mod) ->
    tell_conns(Mod, {service_unavailable, Svc, DLMod}).

tell_conns(Mod, Msg) ->
    Conns = gproc:select(
              {n,l}, [{ {{n,l,{dlink,?MODULE,conn,'$1'}}, '$2', '_'},
                        [], [{{'$1', '$2'}}] }]),
    ?debug("Conns = ~p", [Conns]),
    [gen_server:cast(Pid, Msg) || {_, Pid} <- Conns],
    ok.

setup_connections(Server) ->
    gen_server:cast(Server, setup_connections).

init({Module, Arg} = X) ->
    ?debug("init(~p)", [X]),
    ets_new(Module:table_name(connections), #connection_entry.connection),
    ets_new(Module:table_name(services), #service_entry.service),
    case Module:dlink_init(Arg) of
        {ok, MSt} ->
            {ok, #st{mod = Module,
                     mod_st = MSt}};
        Other ->
            Other
    end.

handle_call(_Reg, _From, St) ->
    {reply, error, St}.

handle_cast(setup_connections, St) ->
    {noreply, setup_connections_(St)}.

%% Setup static nodes
handle_info({rvi_setup_persistent_connection, Args}, St) ->
    ?info("rvi_setup_persistent_connection, ~p~n", [Args]),
    {noreply, connect_and_retry_remote(Args, St)};
handle_info({accepted, Pid, LS}, #st{listeners = Listeners} = St) ->
    case lists:keyfind(LS, #listener.socket, Listeners) of
        #listener{acceptors = As} = L ->
            case lists:member(Pid, As) of
                true ->
                    L1 = L#listener{acceptors = As -- [Pid]},
                    {noreply, start_acceptor(L1, St)};
                false ->
                    {noreply, St}
            end;
        false ->
            {noreply, St}
    end;
handle_info(_Msg, St) ->
    {noreply, St}.

start_acceptor(#listener{socket = LS} = L, #st{listeners = Lis} = St) ->
    L1 = do_start_acceptor(L, St),
    St#st{listeners = lists:keystore(LS, #listener.socket, Lis, L1)}.

do_start_acceptor(#listener{socket = LS, acceptors = As, options = Opts} = L,
                  #st{my_ip = MyIP, my_port = MyPort,
                      mod = Mod, mod_st = ModSt}) ->
    case dlink_conn:start_acceptor(LS, Mod, MyIP, MyPort, Opts) of
        {ok, Pid} when is_pid(Pid) ->
            L#listener{acceptors = [Pid|As]};
        Error ->
            ?debug("Error starting acceptor: ~p", [Error]),
            L
    end.

terminate(_Reason, _St) ->
    ok.

code_change(_FromVsn, St, _Extra) ->
    {ok, St}.

setup_connections_(#st{mod = Mod} = St) ->
    {ok, ServerOpts} = rvi_common:get_module_config(
                         data_link, Mod, ?SERVER_OPTS, [], St),
    ?debug("ServerOpts = ~p", [ServerOpts]),
    St1 = setup_initial_listeners(ServerOpts, St),
    {ok, PersistentConnections} =
        rvi_common:get_module_config(
          data_link, Mod, ?PERSISTENT_CONNECTIONS, [], St),
    setup_persistent_connections_(PersistentConnections, St1).

setup_initial_listeners([], St) ->
    ?debug("no initial listeners", []),
    St;
setup_initial_listeners([_|_] = TlsOpts, CompSpec) ->
    case lists:keytake(ports, 1, TlsOpts) of
        {value, {_, Ports}, Rest} ->
            setup_initial_listeners_(Rest, CompSpec),
            [setup_initial_listeners_(
               [{port,P}|inherit_opts([ip, ifaddr], TlsOpts, POpts)], CompSpec)
             || {P, POpts} <- Ports];
        false ->
            setup_initial_listeners_(TlsOpts, CompSpec)
    end.

inherit_opts(Keys, From, To) ->
    Pick = [{K,V} || {K, V} <- From,
                     lists:member(K, Keys),
                     not lists:keymember(K, 1, To)],
    Pick ++ To.

setup_initial_listeners_(Ls, #st{mod = Mod, mod_st = MSt} = St) ->
    lists:foldl(
      fun([_|_] = Opts, Sx) ->
              {_, Port} = lists:keyfind(port, Opts),
              case Mod:listen(Port, Opts, MSt) of
                  {ok, LS, MSt1} ->
                      start_initial_acceptors(LS, Opts, Sx#st{mod_st = MSt1});
                  Error ->
                      ?debug("~p:listen(~p, ~p, _) -> ~p", [Mod, Port, Opts]),
                      Sx
              end
      end, St, Ls).

start_initial_acceptors(LS, Opts, #st{listeners = Lis} = St) ->
    N = proplists:get_value(acceptors, Opts, ?DEFAULT_ACCEPTORS),
    L0 = #listener{socket = LS, options = Opts},
    lists:foldl(
      fun(_, Lx) ->
              do_start_acceptor(Lx, St)
      end, L0, lists:seq(1, N)).

setup_persistent_connections_(Conns, St) ->
    ?debug("setup_persistent_connections()", []),
    lists:foreach(fun(C) ->
                          ?debug("Conn = ~p", [C]),
                          setup_persistent_connection_(C, St)
                  end, Conns).

setup_persistent_connection_({Address, Options} = C, St)
  when is_list(Address) ->
    [IP, Port] = string:tokens(Address, ":"),
    setup_reconnect_timer(0, {IP, Port, Options});
setup_persistent_connection_(Address, St) when is_list(Address) ->
    [ IP, Port] =  string:tokens(Address, ":"),
    %% cast an immediate (re-)connect attempt to dlink_tls_rpc
    setup_reconnect_timer(0, {IP, Port});
setup_persistent_connection_(Other, _St) ->
    ?error("Invalid persistent connection: ~p", [Other]).

connect_and_retry_remote(Args, #st{persistent_connections = Conns} = St) ->
    {IP, Port, Opts} =
        case Args of
            {I, P}          -> {I, P, []};
            {I, P, Os}      -> {I, P, Os}
        end,
    ?debug("connect_and_retry_remote(): ~p:~p (~p)",
           [IP, Port, Opts]),
    LogId = start_log("connect ~s:~s", [IP, Port], St),
    case connect_remote(IP, list_to_integer(Port), Opts, LogId, St) of
        {ok, Pid} ->
            log("connector running", [], LogId),
            MRef = erlang:monitor(process, Pid),
            P = #pconn{pid = Pid,
                       mref = MRef,
                       ip = IP,
                       port = Port,
                       opts = Opts},
            St#st{persistent_connections = [P | Conns]};
        Err -> %% Failed to connect. Sleep and try again
            ?notice("dlink_tls:connect_and_retry_remote(~p:~p): Failed: ~p",
                           [IP, Port, Err]),
            ?notice("connect_and_retry_remote(~p:~p): Will try again in ~p sec",
                    [IP, Port, ?DEFAULT_RECONNECT_INTERVAL]),
            log("start reconnect timer", [], LogId),
            setup_reconnect_timer(?DEFAULT_RECONNECT_INTERVAL, {IP, Port}),
            St
    end.

connect_remote(IP, Port, Opts, LogId, #st{mod = Mod, mod_st = MSt} = St) ->
    case find_connection_by_address(IP, Port, Mod) of
        Pid when is_pid(Pid) ->
            log("already connected", [], LogId),
            already_connected;
        undefined ->
            %% Setup a new outbound connection
            #st{my_ip = MyIP, my_port = MyPort} = St,
            Timeout =
                rvi_common:get_config(
                  [{option, timeout, Opts},
                   {config, {data_link, ?MODULE, connect_timeout}}],
                  10000),
            Mod:start_connector(
              IP, Port, Timeout, Mod, MyIP, MyPort, [{log_id, LogId},
                                                     {mod_state, MSt}])
    end.

setup_reconnect_timer(MSec, Args) ->
    erlang:send_after(MSec, ?MODULE, {rvi_setup_persistent_connection, Args}),
    ok.

ets_new(Tab, Pos) ->
    ets:new(Tab, [set, public, named_table, {keypos, Pos}]).

add_services(Svcs, Cost, Pid, Mod) ->
    CTab = Mod:table_name(connections),
    STab = Mod:table_name(services),
    ets:insert(CTab,
               #connection_entry{
                  connection = Pid,
                  services = union(Svcs,
                                   get_services_by_connection(Pid, Mod))
                 }),
    [ets:insert(
       STab,
       #service_entry{
          service = SvcName,
          connections = [{Pid, Cost}
                         | get_connections_by_service(SvcName, Mod)]
         }) || SvcName <- Svcs],
    ok.

union(A, B) ->
    A ++ (B -- A).

delete_services(Pid, Svcs, Mod) ->
    CTab = Mod:table_name(connections),
    STab = Mod:table_name(services),
    [ets:insert(
       STab,
       #service_entry{
          service = SvcName,
          connections = [S || {P, _} = S
                                  <- get_connections_by_service(SvcName, Mod),
                              P =/= Pid]
         }) || SvcName <- Svcs],
    ok.

get_connections_by_service(SvcName, Mod) ->
    Svcs = Mod:table_name(services),
    case ets:lookup(Svcs, SvcName) of
        [#service_entry{connections = Conns}] ->
            Conns;
        [] ->
            []
    end.

get_services_by_connection(Pid, Mod) ->
    case ets:lookup(Mod:table_name(connections), Pid) of
        [#connection_entry{services = SvcNames}] ->
            SvcNames;
        [] ->
            []
    end.

delete_connection(Pid, Mod) when is_pid(Pid) ->
    Conns = Mod:table_name(connections),
    Svcs = Mod:table_name(services),
    LostSvcs = case ets:lookup(Conns, Pid) of
                   [#connection_entry{services = SvcNames}] ->
                       SvcNames;
                   [] ->
                       []
               end,
    lists:foreach(
      fun(SvcName) ->
              Existing = get_connections_by_service(SvcName, Mod),
              ets:insert(Svcs, #service_entry{
                                  service = SvcName,
                                  connections = lists:keydelete(
                                                  Pid, 2, Existing)})
      end, LostSvcs),
    ets:delete(Conns, Pid),
    LostSvcs.

%% FIXME! Replace hop count with route list
link_cost(_St) -> 1.

routing_cost(_St) -> 1.

max_cost(_St) -> 10.

get_credentials(CompSpec) ->
    case authorize_rpc:get_credentials(CompSpec) of
	[ok, Creds] ->
	    Creds;
	[not_found] ->
	    ?error("No credentials found~n", []),
	    error(no_credentials_found)
    end.



opt(K, L, Def) ->
    case lists:keyfind(K, 1, L) of
        {_, V} -> V;
        false  -> Def
    end.

opts(Keys, Elems, Def) ->
    [opt(K, Elems, Def) || K <- Keys].

start_log(Fmt, Args, #st{mod = Mod, mod_st = MSt}) ->
    {Pfx, Component} = Mod:log_info(MSt),
    LogId = rvi_log:new_id(Pfx),
    rvi_log:log(LogId, Component, rvi_log:format(Fmt, Args)),
    LogId.

log(Fmt, Args, LogId) ->
    rvi_log:flog(Fmt, Args, <<"dlink_tls">>, LogId).

abbrev(Data) ->
    authorize_keys:abbrev(Data).

find_connection_by_address(IP, Port, Mod) ->
    gproc:where({n, l, {dlink_conn, Mod, IP, Port}}).
