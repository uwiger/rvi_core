%% -*- erlang-indent-level: 4; indent-tabs-mode: nil -*-
%%
%% Copyright (C) 2016, Jaguar Land Rover
%%
%% This program is licensed under the terms and conditions of the
%% Mozilla Public License, version 2.0.  The full text of the
%% Mozilla Public License is at https://www.mozilla.org/MPL/2.0/
%%

%%%-------------------------------------------------------------------
%%% @author Ulf Wiger <uwiger@jaguarlandrover.com>
%%% @copyright (C) 2016, Jaguar Land Rover
%%% @doc
%%%
%%% @end
%%% Created : 22 Jury 2016 by Ulf Wiger <uwiger@jaguarlandrover.com>
%%%-------------------------------------------------------------------
-module(dlink_conn).

-behaviour(gen_server).
-include_lib("lager/include/log.hrl").
-include_lib("public_key/include/public_key.hrl").
-include_lib("rvi_common/include/rvi_dlink_bin.hrl").

%% API

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-export([setup/8]).
-export([start_acceptor/5,
         start_connector/7,
         connections/1]).

-export([send/2]).
-export([send/3]).
-export([send_data/2]).
-export([is_connection_up/1]).
-export([is_connection_up/3]).

-export([find_by_address/3]).

-define(SERVER, ?MODULE).
-define(PACKET_MOD, dlink_data_json).

-record(st, {
          ip = {0,0,0,0},
          port = 0,
          conn,
          sock = undefined,
          server,
          packet_mod = ?PACKET_MOD,
          packet_st = [],
          frag_opts = [],
          mod = undefined,
          mod_st,
          tags = #{},
          func = undefined,
          log_id,
          role = server :: client | server,
          peercert,
          first_announce = false,
          auth_sent = false,
          remote_node_id,
          available = [],
          unavailable = [],
          parent
         }).

%%%===================================================================
%%% API
%%%===================================================================
%% MFA is to deliver data received on the socket.

start_acceptor(LSock, Mod, IP, Port, Args) ->
    gen_server:start_link(?MODULE, {{accept, LSock}, self(),
                                    Mod, IP, Port, Args}).

start_connector(IP, Port, Timeout, Mod, MyIP, MyPort, Args) ->
    gen_server:start_link(?MODULE, {{connect, IP, Port, Timeout}, self(),
                                    Mod, MyIP, MyPort, Args}).

%% ==== RVI handshake protocol

send_AUTHORIZE(#st{mod = Mod, mod_st = MSt} = St) ->
    ?debug("send_AUTHORIZE()", []),
    Creds = get_credentials(),
    Dels = authorize_rpc:get_delegates(),
    Encodings = Mod:encodings(MSt),
    St1 = do_send(
            #{?DLINK_ARG_CMD         => ?DLINK_CMD_AUTHORIZE,
              ?DLINK_ARG_VERSION     => ?DLINK_VERSION,
              ?DLINK_ARG_NODE_ID     => rvi_common:local_service_prefix(),
              ?DLINK_ARG_ENC         => Encodings,
              ?DLINK_ARG_DELEGATES   => Dels,
              ?DLINK_ARG_CREDENTIALS => Creds}, [], St),
    St1#st{auth_sent = true}.

%% FIXME! We should not send subsequent auth messages, but rather
%% introduce cred announcements and encoding change requests.
%%
recv_AUTHORIZE(#{?DLINK_ARG_CMD         := ?DLINK_CMD_AUTHORIZE,
                 ?DLINK_ARG_VERSION     := Vsn,
                 ?DLINK_ARG_NODE_ID     := NodeId,
                 ?DLINK_ARG_CREDENTIALS := Creds} = Msg, St) ->
    ?debug("rcv_AUTHORIZE(~p)", [Msg]),
    case Vsn of
        <<"1.", _/binary>> ->
            ok;
        _ ->
            error({protocol_failure, {unknown_version, Vsn}})
    end,
    authorize_rpc:store_creds(Creds, St#st.conn, St#st.peercert),
    log("authorized ~s:~w", [St#st.ip, St#st.port], Msg),
    St1 = case St#st.auth_sent of
              true ->
                  St;
              false ->
                  send_AUTHORIZE(St)
          end,
    St2 = check_node_id(NodeId, St1#st{auth_sent = false}),
    maybe_announce(set_encoding(Msg, St2)).

check_node_id(NodeId, #st{remote_node_id = undefined, mod = Mod} = St) ->
    schedule_rpc:publish_node_id(NodeId, Mod),
    St#st{remote_node_id = NodeId};
check_node_id(NodeId, #st{remote_node_id = NodeId} = St) ->
    St;
check_node_id(NodeId, #st{remote_node_id = OtherNodeId}) ->
    erlang:error({id_mismatch, [NodeId, OtherNodeId]}).

maybe_announce(#st{first_announce = false} = St) ->
    St;
maybe_announce(#st{conn = Conn} = St) ->
    LocalServices = service_discovery_rpc:get_services_by_module(local),
    FilteredServices = authorize_rpc:filter_by_service(LocalServices, Conn),
    send_ANNOUNCE(
      FilteredServices, ?DLINK_ARG_AVAILABLE, [], St#st{first_announce = true}).

send_ANNOUNCE(Services, Status, Route, St) ->
    NodeId = rvi_common:local_service_prefix(),
    do_send(
      #{?DLINK_ARG_CMD => ?DLINK_CMD_SERVICE_ANNOUNCE,
        ?DLINK_ARG_STATUS => Status,
        ?DLINK_ARG_ROUTE  => [NodeId|Route],
        ?DLINK_ARG_SERVICES => Services}, [],
      update_avail(Services, Status, St)).

recv_ANNOUNCE(#{?DLINK_ARG_STATUS   := Status,
                ?DLINK_ARG_ROUTE    := Route,
                ?DLINK_ARG_SERVICES := Svcs} = Msg,
              #st{available = Avail, unavailable = Unavail} = St) ->
    MyId = rvi_common:local_service_prefix(),
    case lists:member(MyId, Route) of
        true ->
            ?debug("announcement loop; ignore ~p", [Msg]),
            St;
        false ->
            case Status of
                ?DLINK_ARG_AVAILABLE ->
                    service_discovery_rpc:register_services(Svcs -- Avail),
                    St#st{available = union(Avail, Svcs),
                          unavailable = Unavail -- Svcs};
                ?DLINK_ARG_UNAVAILABLE ->
                    service_discovery_rpc:unregister_services(Svcs -- Unavail),
                    St#st{available = Avail -- Svcs,
                          unavailable = union(Unavail, Svcs)}
    end.

update_avail(Svcs, ?DLINK_ARG_AVAILABLE, #st{unavailable = Un,
                                             available   = Av} = St) ->
    St#st{unavailable = Un -- Svcs,
          available   = union(Av, Svcs)}.

union(L1, L2) ->
    L1 ++ (L2 -- L1).

set_encoding(#{?DLINK_ARG_ENC := Enc},
             #st{role = Role, mod = Mod, mod_st = ModSt} = St) ->
    %% Pick the encoding most preferred by the client, which is also supported
    %% by the server
    MyOpts = [_|_] = Mod:encodings(ModSt),
    {A, B} = case Role of
                 client -> {MyOpts, Enc};
                 server -> {Enc, MyOpts}
             end,
    case lists:dropwhile(fun(E) -> not lists:member(E, B) end, A) of
        [Pick|_] -> set_encoding_(Pick, St);
        []       -> set_encoding_(<<"json">>, St)
    end;
set_encoding(_, St) ->
    %% No encoding option offered by peer - default to json
    set_encoding_(<<"json">>, St).

set_encoding_(Name, St) ->
    Mod = case Name of
              <<"json">>    -> dlink_data_json;
              <<"msgpack">> -> dlink_data_msgpack
          end,
    PktSt = Mod:init(),
    St#st{packet_mod = Mod, packet_st = []}.

get_credentials() ->
    case authorize_rpc:get_credentials() of
        {ok, Creds} ->
            Creds;
        Other ->
            ?debug("ERROR: get_credentials() -> ~p", [Other]),
            []
    end.

setup(Server, Role, IP, Port, Sock, Mod, Fun, CompSpec) when Role==client;
							     Role==server ->
    Params = {Server, Role, IP, Port, Sock, Mod, Fun, CompSpec},
    ?debug("setup() Server = ~p; IP = ~p; Port = ~p; Mod = ~p; Fun = ~p",
	   [Server, IP, Port, Mod, Fun]),
    ?debug("CompSpec = ~p", [CompSpec]),
    gen_server:start_link(?MODULE, Params, []).

send(Pid, Data) when is_pid(Pid) ->
    gen_server:cast(Pid, {send, Data}).

send(Pid, Data, Opts) when is_pid(Pid) ->
    gen_server:cast(Pid, {send, Data, Opts});
send(IP, Port, Data) ->
    case dlink_tls_connmgr:find_connection_by_address(IP, Port) of
	{ok, Pid} ->
	    gen_server:cast(Pid, {send, Data});

	_Err ->
	    ?info("connection:send(): Connection ~p:~p not found for data: ~p",
		  [ IP, Port, Data]),
	    not_found

    end.

send_data(Pid, Data) ->
    gen_server:cast(Pid, {send_data, Data}).

is_connection_up(Pid) when is_pid(Pid) ->
    is_process_alive(Pid).

is_connection_up(Mod, IP, Port) ->
    case find_by_address(Mod, IP, Port) of
        Pid when is_pid(Pid) -> true;
        undefined            -> false
    end.

find_by_address(Mod, IP, Port) ->
    gproc:where({n, l, {?MODULE, Mod, IP, Port}}).



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
%% MFA used to handle socket closed, socket error and received data
%% When data is received, a separate process is spawned to handle
%% the MFA invocation.

init({Op, Parent, Mod, IP, Port, Args} = _X) ->
    ?debug("connector starting: ~p", [_X]),
    LogId = proplists:get_value(log_id, Args),
    MSt0 = proplists:get_value(mod_state, Args),
    case Mod:conn_init(Op, IP, Port, MSt0) of
        {ok, MSt} ->
            Tags = Mod:socket_tags(MSt),
            proc_lib:init_ack(Parent, {ok, self()}),
            init_cont(Op, #st{ip = IP,
                              port = Port,
                              log_id = LogId,
                              mod = Mod,
                              mod_st = MSt,
                              tags = Tags,
                              parent = Parent});
        Other ->
            Other
    end.

init_cont({accept, LS}, #st{mod = Mod, mod_st = MSt,
                            parent = Parent} = St) ->
    {ok, S, MSt1} = Mod:accept(LS, MSt),
    Parent ! {accepted, self(), LS},
    St1 = handle_upgrade(connected(server, S, MSt1, St)),
    loop(St1);
init_cont({connect, IP, Port, Timeout}, #st{mod = Mod, mod_st = MSt} = St) ->
    {ok, S, MSt1} = Mod:connect(IP, Port, Timeout, MSt),
    St1 = handle_upgrade(connected(client, S, MSt1, St)),
    loop(send_AUTHORIZE(St1)).

connected(Role, Sock, MSt, #st{mod = Mod} = St) ->
    St#st{role = Role, mod_st = MSt, sock = Sock}.

loop(St) ->
    gen_server:enter_loop(?MODULE, [], St).


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

handle_call(_Request, _From, #st{} = State) ->
    ?warning("~p:handle_call(): Unknown call: ~p", [ ?MODULE, _Request]),
    Reply = ok,
    {reply, Reply, State}.

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
handle_cast({send, Data},  #st{packet_mod = PMod, packet_st = PSt,
                               mod = Mod, mod_st = MSt} = St) ->
    ?debug("handle_cast(send): Sending: ~p", [abbrev(Data)]),
    St1 = do_send(Data, [], St),
    {noreply, St1};
handle_cast({send, Data, Opts} = Req, #st{} = St) ->
    ?debug("handle_cast(~p, ...)", [Req]),
    St1 = do_send(Data, Opts, St),
    {noreply, St1};
handle_cast({send_data, Data}, #st{sock = S} = St) ->
    %% don't encode; just send. Used by frag support.
    ?debug("send_data, ~w", [authorize_keys:abbrev_bin(Data)]),
    ssl:send(S, Data),
    {noreply, St};
%% handle_cast({service_available, {SvcName, local}}, St) ->
%%     ?debug("service_available(): ~p (local)", [SvcName]),
%%     {noreply, announce_local_service_(SvcName, available, St)};
%% handle_cast({service_available, _SvcName, _Mod}, St) ->
%%     ?debug("service_available(): ~p (~p) ignored", [_SvcName, _Mod]),
%%     %% We don't care about remote services available through
%%     %% other data link modules
%%     {noreply, St};
%% handle_cast({service_unavailable, SvcName, local}, St) ->
%%     ?debug("service_unavailable(): ~p (local)", [SvcName]),
%%     {noreply, announce_local_service_(SvcName, unavailable, St)};
%% handle_cast({service_unavailable, _SvcName, _Mod}, St) ->
%%     ?debug("service_unavailable(): ~p (~p) ignored", [_SvcName, _Mod]),
%%     %% We don't care about remote services available through
%%     %% other data link modules
%%     {noreply, St};
%% %%
handle_cast(_Msg, #st{} = State) ->
    ?warning("~p:handle_cast(): Unknown cast: ~p~nSt=~p", [ ?MODULE, _Msg, State]),
    {noreply, State}.

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

handle_info({ssl, Sock, Data}, #st{ip = IP, port = Port,
                                   packet_mod = PMod,
                                   packet_st = PSt} = St) ->
    ?debug("handle_info(data): Data: ~p",     [abbrev(Data)]),
    ?debug("handle_info(data): From: ~p:~p ", [IP, Port]),
    ?debug("handle_info(data): PMod: ~p",     [PMod]),
    case PMod:decode(Data, fun(Elems) ->
                                   handle_msg(Elems, St)
                           end, PSt) of
        {ok, PSt1} ->
            ssl:setopts(Sock, [{active, once}]),
            {noreply, St#st{packet_st = PSt1}};
        {error, Reason} ->
            {stop, Reason, St}
    end;
handle_info({Tag, Sock, Data}, #st{tags = #{data := Tag},
                                   ip = IP,
                                   port = Port,
                                   mod = Mod,
                                   mod_st = MSt} = St) ->
    ?debug("got unexpected data", []),
    ?debug("handle_info(data): Data: ~p", [Data]),
    ?debug("handle_info(data): From: ~p:~p ", [IP, Port]),
    {stop, unexpected_data, St};
handle_info({ssl_closed, Sock} = Evt, #st{} = St) ->
    close_event(Sock, St);
handle_info({Tag, Sock} = Evt, #st{tags = #{closed := Tag}} = St) ->
    close_event(Sock, St);
handle_info({ssl_error, Sock}, #st{tags = #{error := Tag}} = St) ->
    error_event(Sock, St);
handle_info(_Info, #st{} = State) ->
    ?warning("~p:handle_cast(): Unknown info: ~p", [ ?MODULE, _Info]),
    {noreply, State}.


close_event(Sock, #st{ip = IP, port = Port, mod = Mod, mod_st = MSt} = St) ->
    ?debug("close_event(): Address: ~p:~p", [IP, Port]),
    Mod:closed(Sock, MSt),
    try ssl:close(Sock) catch _:_ -> ok end,
    {stop, normal, St}.

error_event(Sock, #st{ip = IP, port = Port, mod = Mod, mod_st = MSt} = St) ->
    ?debug("error_event(): Address: ~p:~p", [IP, Port]),
    Mod:closed(Sock, MSt),
    try ssl:close(Sock) catch _:_ -> ok end,
    {stop, normal, St}.


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
    ?debug("~p:terminate(): Reason: ~p ", [ ?MODULE, _Reason]),
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

do_send(Data, Opts, #st{sock = S, frag_opts = FragOpts,
                        packet_mod = PMod, packet_st = PSt} = St) ->
    {ok, Bin, PSt1} = PMod:encode(Data, PSt),
    St1 = St#st{packet_st = PSt1},
    rvi_frag:send(Bin, Opts ++ FragOpts, ?MODULE, fun() ->
                                                          ssl:send(S, Bin)
                                                  end),
    St1.

handle_upgrade(#st{role = Role} = St) ->
    handle_upgrade(Role, rvi_common:get_component_spec(), St).

handle_upgrade(Role, CompSpec, #st{mod = Mod, mod_st = MSt, sock = S} = St) ->
    {_, MSt1} = Mod:setopts(S, [{active, false}], MSt),
    St1 = St#st{mod_st = MSt1},
    case do_upgrade(S, Role, St1) of
	{DoVerify, {ok, NewS}} ->
	    ?debug("upgrade to TLS succcessful~n", []),
            ssl:setopts(NewS, [{active, once}]),
            {ok, {IP0, Port}} = ssl:peername(NewS),
            IP = inet_parse:ntoa(IP0),
            Conn = {IP, Port},
            register_connection(Conn, Mod),
            PeerCert = get_peercert(DoVerify, NewS),
            ?debug("SSL PeerCert=~w", [abbrev(PeerCert)]),
	    {ok, St#st{sock = NewS, role = Role,
                       ip = IP, port = Port,
                       conn = Conn,
                       peercert = PeerCert}};
	{_, Error} ->
	    ?error("Cannot upgrade to TLS: ~p~n", [Error]),
            error({cannot_upgrade, Error})
    end.

register_connection(Conn, Mod) ->
    gproc:reg({n, l, {dlink, Mod, conn, Conn}}).

connections(Mod) ->
    gproc:select({n,l}, [{ {{n, l, {dlink, Mod, conn, '$1'}}, '$2', '_'},
                           [], [ {{'$1', '$2'}} ] }]).

get_peercert(DoVerify, S) ->
    case ssl:peercert(S) of
        {ok, PeerCert} ->
            PeerCert;
        {error, _} when DoVerify == false ->
            undefined
    end.

do_upgrade(Sock, client, St) ->
    {DoVerify, Opts} = tls_opts(client, St),
    ?debug("TLS Opts = ~p", [Opts]),
    {DoVerify, ssl:connect(Sock, Opts)};
do_upgrade(Sock, server, St) ->
    {DoVerify, Opts} = tls_opts(server, St),
    ?debug("TLS Opts = ~p", [Opts]),
    {DoVerify, ssl:ssl_accept(Sock, Opts)}.

tls_opts(Role, #st{mod = Mod, mod_st = MSt}) ->
    TlsOpts0 = Mod:tls_opts(MSt),
    TlsOpts = TlsOpts0 ++
        [{reuse_sessions, false}
         || not lists:keymember(reuse_sessions, 1, TlsOpts0)],
    ?debug("TlsOpts = ~p", [TlsOpts]),
    Opt = fun(K) -> opt(K, TlsOpts,
                        fun() ->
                                ok(setup:get_env(rvi_core, K))
                        end)
          end,
    case VOpt = lists:keyfind(verify, 1, TlsOpts) of
        {verify, false} when Role == server ->
            {false, [
                     {verify, verify_none},
                     {certfile, Opt(device_cert)},
                     {keyfile, Opt(device_key)},
                     {cacertfile, Opt(root_cert)}
                     | other_tls_opts(TlsOpts)]};
        {verify, false} ->
            {false, [
                     {verify, verify_none}
                     | other_tls_opts(TlsOpts)]};
        _ when VOpt==false; VOpt == {verify, true} ->  % {verify,true} default
            {true, [
                    {verify, verify_peer},
                    {certfile, Opt(device_cert)},
                    {keyfile, Opt(device_key)},
                    {cacertfile, Opt(root_cert)},
                    {verify_fun, opt(verify_fun, TlsOpts,
                                     {fun verify_fun/3, public_root_key()})},
                    {partial_chain, opt(partial_chain, TlsOpts,
                                        fun(X) ->
                                                partial_chain(Role, X)
                                        end)}
                    | other_tls_opts(TlsOpts)
                   ]}
    end.

other_tls_opts(Opts) ->
    other_tls_opts([device_cert, device_key,
                    root_cert, verify_fun,
                    partial_chain, verify], Opts).

other_tls_opts(Remove, Opts) ->
    [O || {K,_} = O <- Opts,
          not lists:member(K, Remove)].

opt(Key, Opts, Def) ->
    case lists:keyfind(Key, 1, Opts) of
        false when is_function(Def, 0) -> Def();
        false  -> Def;
        {_, V} -> V
    end.

ok({ok, V}) ->
    V;
ok(Other) ->
    error({badmatch, Other}).

public_root_key() ->
    authorize_keys:provisioning_key().

verify_fun(Cert, What, St) ->
    ?debug("verify_fun(~p, ~p, ~p)", [abbrev(Cert), What, abbrev(St)]),
    verify_fun_(Cert, What, St).

verify_fun_(Cert, {bad_cert, selfsigned_peer}, PubKey) ->
    ?debug("Verify self-signed cert: ~p", [abbrev(Cert)]),
    try verify_cert_sig(Cert, PubKey) of
        true ->
            ?debug("verified!", []),
            {valid, PubKey};
        false ->
            ?debug("verification FAILED", []),
            {bad_cert, invalid_signature}
    catch
        error:Error ->
            ?debug("Caught error:~p~n~p", [Error, erlang:get_stacktrace()]),
            {fail, PubKey}
    end;
verify_fun_(_, {bad_cert, Reason}, St) ->
    ?debug("Bad cert: ~p", [Reason]),
    {fail, St};
verify_fun_(_, {extension, _}, St) ->
    {unknown, St};
verify_fun_(_, valid, St) ->
    {valid, St};
verify_fun_(_, valid_peer, St) ->
    {valid_peer, St}.

partial_chain(_, Certs) ->
    ?debug("partial_chain() invoked, length(Certs) = ~w", [length(Certs)]),
    Decoded = (catch [public_key:der_decode('Certificate', C)
                      || C <- Certs]),
    ?debug("partial_chain: ~p", [[lager:pr(Dec) || Dec <- Decoded]]),
    {trusted_ca, hd(Certs)}.

handle_msg(Msg, #st{frag_opts = FragOpts} = St) ->
    MaybeF = rvi_frag:maybe_fragment(Msg, ?MODULE, FragOpts),
    ?debug("maybe_fragment(~p) -> ~p", [Msg, MaybeF]),
    case MaybeF of
        true ->
            %% It was a fragment, but not a complete message yet
            St;
        {true, Msg1} ->
            #st{packet_mod = PMod, packet_st = PSt} = St,
            PMod:decode(Msg1, fun(FinalMsg) ->
                                     got_msg(FinalMsg, St)
                             end, PSt);
        false ->
            got_msg(Msg, St)
    end.

got_msg(#{?DLINK_ARG_CMD := ?DLINK_CMD_AUTHORIZE} = Msg, St) ->
    recv_AUTHORIZE(Msg, St);
got_msg(#{?DLINK_ARG_CMD := ?DLINK_CMD_SERVICE_ANNOUNCE} = Msg, St) ->
    recv_ANNOUNCE(Msg, St);
got_msg(#{?DLINK_ARG_CMD := ?DLINK_CMD_RECEIVE} = Msg, St) ->
    recv_MESSAGE(Msg, St);
got_msg(#{?DLINK_ARG_CMD := ?DLINK_CMD_PING} = Msg, St) ->
    St;
got_msg(#{} = Msg, #st{ip = IP, port = Port, mod = Mod, func = Fun} = St) ->
    ?debug("got_msg() UNKNOWN: ~p", [Msg]),
    St.

verify_cert_sig(#'OTPCertificate'{tbsCertificate = TBS,
				  signature = Sig}, PubKey) ->
    DER = public_key:pkix_encode('OTPTBSCertificate', TBS, otp),
    {SignType, _} = signature_algorithm(TBS),
    public_key:verify(DER, SignType, Sig, PubKey).

signature_algorithm(#'OTPCertificate'{tbsCertificate = TBS}) ->
    signature_algorithm(TBS);
signature_algorithm(#'OTPTBSCertificate'{
		       signature = #'SignatureAlgorithm'{
				      algorithm = Algo}}) ->
    public_key:pkix_sign_types(Algo).


abbrev(T) ->
    authorize_keys:abbrev(T).

log(Fmt, Args, Msg) ->
    rvi_log:format(Fmt, Args, <<"dlink">>, Msg).

%% Service announcements
announce_local_service_(Service, Availability, Mod, LogId) ->
    %% FIXME! Use route instead of cost. Also filter on auth in
    %% service_discovery instead
    announce_services_(
      connections(Mod), [Service], 1, Availability, Mod, LogId).

announce_services_(Conns, Services, Cost, Availability, Mod, LogId) ->
    [announce_service_(C, Services, Cost, Availability, Mod, LogId)
     || C <- Conns],
    ok.

announce_service_({Conn, Pid} = C, Services, Cost, Availability, Mod, LogId) ->
    ?debug("announce_services(~p, ~p, ~p)", [Conn, Pid, Services]),
    case authorize_rpc:filter_by_service(Services, Conn) of
	{ok, [_]} ->
	    ?debug("will announce", []),
	    Res = dlink_conn:send(
		    Pid,
                    #{?DLINK_ARG_CMD      => ?DLINK_CMD_SERVICE_ANNOUNCE,
                      ?DLINK_ARG_COST     => Cost,
                      ?DLINK_ARG_STATUS   => status_string(Availability),
                      ?DLINK_ARG_SERVICES => Services,
                      ?DLINK_ARG_LOG_ID   => LogId}),
	    ?debug("announce_services(~p: ~p) -> ~p  Res: ~p",
		   [Availability, Services, C, Res]);
	_Other ->
	    ?debug("WON'T announce (~p)", [_Other]),
	    ignore
    end,
    ok.

status_string(available  ) -> ?DLINK_ARG_AVAILABLE;
status_string(unavailable) -> ?DLINK_ARG_UNAVAILABLE.
