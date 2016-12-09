%%
%% Copyright (C) 2016, Jaguar Land Rover
%%
%% This program is licensed under the terms and conditions of the
%% Mozilla Public License, version 2.0.  The full text of the
%% Mozilla Public License is at https://www.mozilla.org/MPL/2.0/
%%

-module(dlink_gen_rpc).
-behavior(gen_server).

-export([handle_rpc/2,
         handle_notification/2,
         handle_socket/6,
         handle_socket/5]).

-export([start_link/3,
         init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-export([start_json_server/1,
         start_connection_manager/1]).

-include_lib("lager/include/log.hrl").
-include_lib("rvi_common/include/rvi_common.hrl").
-include_lib("rvi_common/include/rvi_dlink_bin.hrl").

-record(service_entry, {
          service = [],
          connections = []}).

-record(connection_entry, {
          connection = undefined :: pid() | undefined,
          services = []
         }).

-record(st, {
          mod,
          mod_st,
          cs = #component_spec{}
        }).

start_link(Name, Module, Arg) ->
    gen_server:start_link({local, Name}, ?MODULE, {Module, Arg}, []).

setup_connections(Server) ->
    gen_server:cast(Server, setup_connections).

init({Module, Arg} = X) ->
    ?debug("init(~p)", [X]),
    ets_new(Module:table_name(connections), #connection_entry.connection),
    ets_new(Modlue:table_name(services), #service_entry.service),
    CS = rvi_common:get_component_specifications(),
    case Module:init(Arg) of
        {ok, MSt} ->
            {ok, #st{mod = Module,
                     mod_st = MSt,
                     cs = CS}};
        Other ->
            Other
    end.

handle_call(_Reg, _From, St) ->
    {reply, error, St}.

handle_cast(setup_connections, St) ->
    {noreply, setup_connections_(St)}.

%% Setup static nodes
handle_info({rvi_setup_persistent_connection, Args}, St) ->
    ?info("rvi_setup_persistent_connection, ~p, ~p~n", [IP, Port]),
    connect_and_retry_remote(Args),
    { noreply, St };

handle_info(_Msg, St) ->
    {noreply, St}.

terminate(_Reason, _St) ->
    ok.

code_change(_FromVsn, St, _Extra) ->
    {ok, St}.

setup_connections_(#st{mod = Mod, cs = CS} = St) ->
    {ok, ServerOpts} = rvi_common:get_module_config(
                         data_link, Mod, ?SERVER_OPTS, [], CS),
    ?debug("ServerOpts = ~p", [ServerOpts]),
    setup_initial_listeners(ServerOpts, St),
    {ok, PersistentConnections} =
        rvi_common:get_module_config(
          data_link, Mod, ?PERSISTENT_CONNECTIONS, [], CS),
    setup_persistent_connections_(PersistentConnections, CS),
    St.

setup_persistent_connections_(Conns, CompSpec) ->
    ?debug("setup_persistent_connections()", []),
    lists:foreach(fun(C) ->
                          ?debug("Conn = ~p", [C]),
                          setup_persistent_connection_(C, CompSpec)
                  end, Conns).

setup_persistent_connection_({Address, Options} = C, CompSpec)
  when is_list(Address) ->
    [IP, Port] = string:tokens(Address, ":"),
    setup_reconnect_timer(0, {IP, Port, Options, CompSpec});
setup_persistent_connection_(Address, CompSpec) when is_list(Address) ->
    [ IP, Port] =  string:tokens(Address, ":"),
    %% cast an immediate (re-)connect attempt to dlink_tls_rpc
    setup_reconnect_timer(0, {IP, Port, CompSpec});
setup_persistent_connection_(Other, _CompSpec) ->
    ?error("Invalid persistent connection: ~p", [Other]).

connect_and_retry_remote(Args) ->
    {IP, Port, CompSpec, Opts} =
        case Args of
            {I, P, Cs}          -> {I, P, [], Cs};
            {I, P, Os, Cs}      -> {I, P, Os, Cs}
        end,
    ?info("connect_and_retry_remote(): ~p:~p (~p)",
          [IP, Port, Opts]),
    CS = start_log(<<"conn">>, "connect ~s:~s", [IP, Port], CompSpec),
    case connect_remote(IP, list_to_integer(Port), Opts, CS) of
        ok ->
            log("connected", [], CS),
            ok;

        Err -> %% Failed to connect. Sleep and try again
            ?notice("dlink_tls:connect_and_retry_remote(~p:~p): Failed: ~p",
                           [IP, Port, Err]),

            ?notice("connect_and_retry_remote(~p:~p): Will try again in ~p sec",
                    [IP, Port, ?DEFAULT_RECONNECT_INTERVAL]),
            log("start reconnect timer", [], CS),
            setup_reconnect_timer(?DEFAULT_RECONNECT_INTERVAL, {IP, Port, CS}),

            not_available
    end.

connect_remote(IP, Port, Opts, CompSpec) ->
    ?info("connect_remote(~p, ~p)~n", [IP, Port]),
    case dlink_connmgr:find_connection_by_address(IP, Port) of
        { ok, _Pid } ->
            log("already connected", [], CompSpec),
            already_connected;

        not_found ->
            %% Setup a new outbound connection
            Timeout =
                rvi_common:get_config(
                  [{option, timeout, Opts},
                   {config, {data_link, ?MODULE, connect_timeout}, CompSpec}],
                  10000),

            ?info("dlink_tls:connect_remote(): Connecting ~p:~p (TO=~p",
                  [IP, Port, Timeout]),
            log("new connection", [], CompSpec),
            case gen_tcp:connect(IP, Port, dlink_tls_listener:sock_opts(),
                                 Timeout) of
                { ok, Sock } ->
                    ?info("dlink_tls:connect_remote(): Connected ~p:~p",
                           [IP, Port]),

                    %% Setup a genserver around the new connection.
                    {ok, Pid } = dlink_tls_conn:setup(
                                   client, IP, Port, Sock,
                                   ?MODULE, handle_socket, CompSpec),
                    try dlink_conn:upgrade(Pid, client) of
                        ok ->
                            ?debug("Upgrade result = ~p", [ok]),
                            %% Send authorize
                            send_authorize(Pid, CompSpec),
                            ok
                    catch
                        error:Error ->
                            ?error("TLS upgrade (~p,~p) failed ~p",
                                   [IP, Port, Error]),
                            not_available
                    end;
                {error, Err } ->
                    ?info("dlink_tls:connect_remote(): Failed ~p:~p: ~p",
                           [IP, Port, Err]),
                    log("connect FAILED: ~w", [Err], CompSpec),
                    not_available
            end
    end.


handle_socket(FromPid, Host, Port, closed, CS) ->
    ?debug("closed(): Addr: {~p, ~p}", [Host, Port]),
    Mod = get_module(CS),
    Addr = Host ++ ":" ++ integer_to_list(Port),
    LostSvcs = get_services_by_connection(FromPid),
    delete_connection(FromPid),
    SvcsToUnreg =
        [S || S <- LostSvcs,
              get_connections_by_service(S) =:= []],
    service_discovery_rpc:unregister_services(CS, SvcToUnreg, Mod),
    {ok, PersistentConnections} =
        rvi_common:get_module_config(
          data_link, Mod, persistent_connections, [], CS),
    case addr_is_member(Addr, PersistentConnections) of
        true ->
            ?info("closed(): Reconnect address:  ~p", [Addr]),
            ?info("closed(): Reconnect interval: ~p",
                  [?DEFAULT_RECONNECT_INTERVAL ]),
            setup_reconnect_timer(?DEFAULT_RECONNECT_INTERVAL,
                                  {Host, Port, CS});
        false ->
            ok
    end,

handle_socket(FromPid, Host, Port, error, _CS) ->
    ?debug("socket_error(): Addr: {~p, ~p}", [Host, Port]),
    log_orphan(<<"sock">>, "socket ERROR ~s:~w", [Host, Port]),
    ok.

addr_is_member(Addr, Conns) ->
    lists:member(Addr, Conns) orelse
        lists:keymember(Addr, 1, Conns).

setup_reconnect_timer(MSec, Args) ->
    erlang:send_after(MSec, ?MODULE, {rvi_setup_persistent_connection, Args})
    ok.

ets_new(Tab, Pos) ->
    ets:new(Tab, [set, public, named_table, {keypos, Pos}]).

start_log(Pfx, Fmt, Args, CS) ->
    LogId = rvi_log:new_id(Pfx),
    rvi_log:log(LogId, <<"dlink_tls">>, rvi_log:format(Fmt, Args)),
    rvi_common:set_value(rvi_log_id, LogId, CS).

log(Fmt, Args, CS) ->
    rvi_log:flog(Fmt, Args, <<"dlink_tls">>, CS).

abbrev(Data) ->
    authorize_keys:abbrev(Data).
