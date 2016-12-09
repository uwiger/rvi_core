%%
%% Copyright (C) 2014, Jaguar Land Rover
%%
%% This program is licensed under the terms and conditions of the
%% Mozilla Public License, version 2.0.  The full text of the
%% Mozilla Public License is at https://www.mozilla.org/MPL/2.0/
%%


-module(authorize_rpc).
-behaviour(gen_server).

-export([start_link/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
	 terminate/2, code_change/3]).

-export([get_credentials/0,
	 get_delegates/0,
	 sign_message/1,
	 validate_message/2,
	 store_creds/2,
	 store_creds/3,
	 remove_connection/1,
	 authorize_local_message/1,
	 authorize_remote_message/1]).
-export([filter_by_service/2]).

%% for service_discovery notifications
-export([service_available/2,
	 service_unavailable/2]).

%% for testing & development
-export([public_key/0, public_key_json/0,
	 private_key/0]).

-include_lib("lager/include/log.hrl").
-include_lib("rvi_common/include/rvi_common.hrl").

-define(SERVER, ?MODULE).
-record(st, {
	  next_transaction_id = 1, %% Sequentially incremented transaction id.
	  services_tid = undefined, %% Known services.
	  cs = #component_spec{},
	  private_key = undefined,
	  public_key = undefined
	 }).

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

init([]) ->
    ?debug("authorize_rpc:init(): called."),
    {Priv, Pub} =  authorize_keys:get_device_key(),
    ?debug("KeyPair = {~s, ~s}~n", [authorize_keys:pp_key(Priv),
				    authorize_keys:pp_key(Pub)]),
    service_discovery_rpc:subscribe(?MODULE),
    {ok, #st { cs = rvi_common:get_component_specification(),
	       private_key = Priv,
	       public_key = Pub} }.

sign_message(Message) ->
    ?debug("sign_message()~n", []),
    gen_server:call(?MODULE, {sign_message, Message}).

validate_message(JWT, Conn) ->
    ?debug("validate_message()~n", []),
    gen_server:call(?MODULE, {validate_message, JWT, Conn}).

get_credentials() ->
    ?debug("get_credentials()~n", []),
    gen_server:call(?MODULE, get_credentials).

get_delegates() ->
    ?debug("get_delegates()", []),
    gen_server:call(?MODULE, get_delegates).

remove_connection(Conn) ->
    gen_serger:call(?MODULE, {remove_connection, Conn}).

store_creds(Creds, Conn) ->
    store_creds(Creds, Conn, undefined).

store_creds(Creds, Conn, PeerCert) ->
    ?debug("store_creds(), PeerCert = ~p", [authorize_keys:abbrev(PeerCert)]),
    gen_server:call(?MODULE, {store_creds, Creds, Conn, PeerCert}).

authorize_local_message(#{<<"service_name">> := SvcName} = R) ->
    case authorize_keys:validate_service_call(SvcName) of
	invalid ->
	    log(R, error, "local msg REJECTED", []),
	    false;
	{ok, Id} ->
	    log(R, result, "local msg allowed: Cred=~s", [Id]),
	    true
    end.

authorize_remote_message(#{<<"service_name">> := SvcName,
			   connection := #{ip := IP,
					   port := Port}} = R) ->
    case authorize_keys:validate_service_call(SvcName, {IP, Port}) of
	invalid ->
	    log(R, error, "remote msg REJECTED", []),
	    false;
	{ok, CredID} ->
	    ?debug("validated Cred ID=~p", [CredID]),
	    log(R, result, "remote msg allowed: Cred=~s", [CredID]),
	    true
    end;
authorize_remote_message(#{} = R) ->
    log(R, error, "remote msg REJECTED", []),
    false.


filter_by_service(Services, Conn) ->
    ?debug("filter_by_service(): services: ~p ~n", [Services]),
    ?debug("filter_by_service(): conn: ~p ~n", [Conn]),
    call({filter_by_service, Services, Conn}).

service_available(SvcName, _DLMod) ->
    cast({service_available, SvcName}).

service_unavailable(SvcName, _DLMod) ->
    cast({service_unavailable, SvcName}).

public_key()		-> call(public_key).
public_key_json()	-> call(public_key_json).
private_key()		-> call(private_key).


call(R)		-> gen_server:call(?SERVER, R).
cast(Msg)	-> gen_server:cast(?SERVER, Msg).

%%
%% Gen_server implementation
%%
handle_call({sign_message, [Msg | LogId]}, _, #st{private_key = Key} = State) ->
    Sign = authorize_sig:encode_jwt(Msg, Key),
    log(LogId, result, "signed", []),
    {reply, Sign, State};
handle_call({validate_message, [JWT, Conn | LogId]}, _, State) ->
    try  begin Res = authorize_keys:validate_message(JWT, Conn),
	       log(LogId, result, "validated", []),
	       {reply, {ok, Res}, State}
	 end
    catch
	error:_Err ->
	    log(LogId, error, "validation FAILED", []),
	    {reply, {error, not_found}, State}
    end;
handle_call(get_credentials, _From, State) ->
    {reply, authorize_keys:get_credentials(), State};
handle_call(get_delegates, _From, State) ->
    %% FIXME! Implement delegate handling
    {reply, [], State};
handle_call({store_creds, [Creds, Conn, PeerCert | LogId]}, _From, State) ->
    do_store_creds(Creds, Conn, PeerCert, LogId),
    {reply, ok, State};
handle_call({store_delegates, Dels, Conn, PeerCert, LogId}, _From, State) ->
    do_store_delegates(Dels, Conn, PeerCert, LogId),
    {reply, ok, State};
handle_call({filter_by_service, Services, Conn}, _From, State) ->
    Filtered = authorize_keys:filter_by_service(Services, Conn),
    {reply, Filtered, State};

handle_call({sign, Term}, _From, #st{private_key = Key} = State) ->
    {reply, authorize_sig:encode_jwt(Term, Key), State};

handle_call(public_key, _From, #st{public_key = Key} = State) ->
    {reply, Key, State};

handle_call(public_key_json, _From, #st{public_key = Key} = State) ->
    {reply, authorize_keys:public_key_to_json(Key), State};

handle_call(private_key, _From, #st{private_key = Key} = State) ->
    {reply, Key, State};

handle_call(Other, _From, State) ->
    ?warning("authorize_rpc:handle_call(~p): unknown", [ Other ]),
    {reply, {error, unknown_command}, State}.

handle_cast({service_available, Svc}, State) ->
    authorize_keys:cache_authorizations(Svc),
    {noreply, State};
handle_cast({service_unavailable, Svc}, State) ->
    authorize_keys:remove_cached_authorizations(Svc),
    {noreply, State};
handle_cast({remove_connection, Conn}, State) ->
    authorize_keys:remove_connection(Conn),
    {noreply, State};
handle_cast(Other, State) ->
    ?warning("authorize_rpc:handle_cast(~p): unknown", [ Other ]),
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

do_store_creds(Creds, Conn, PeerCert, LogId) ->
    ?debug("Storing ~p creds for conn ~p~nPeerCert = ~w",
	   [length(Creds), Conn, authorize_keys:abbrev(PeerCert)]),
    authorize_keys:remove_cached_authorizations_for_conn(Conn),
    lists:foreach(fun(Cred) ->
			  store_cred(Cred, Conn, PeerCert, LogId)
		  end, Creds),
    authorize_keys:update_authorization_cache(Conn).

do_store_delegates(Dels, Conn, PeerCert, LogId) ->
    ?debug("storing ~p delegates for conn ~p~nPeerCert = ~w",
	   [length(Dels), Conn, authorize_keys:abbrev(PeerCert)]),
    lists:foreach(fun(Del) ->
			  store_delegate(Del, Conn, PeerCert, LogId)
		  end, Dels).

store_cred(CredJWT, Conn, PeerCert, LogId) ->
    case authorize_sig:decode_jwt(authorize_keys:strip_nl(CredJWT), authorize_keys:provisioning_key()) of
	{_CHeader, CredStruct} ->
	    case authorize_keys:save_cred(CredStruct, CredJWT, Conn, PeerCert, LogId) of
		ok ->
		    ok;
		{error, Reason} ->
		    ?warning(
		       "Couldn't store credential from ~p: ~p~n",
		       [Conn, Reason]),
		    ok
	    end;
	invalid ->
	    log(LogId, warning, "credential INVALID (~p)", [Conn]),
	    ok
    end.

store_delegate(_DelJWT, _Conn, _PeerCert, _LogId) ->
    %% FIXME! Implement delegate handling
    error(nyi).

log([ID], Lvl, Fmt, Args) ->
    log_(ID, Lvl, Fmt, Args);
log(#{} = R, Lvl, Fmt, Args) ->
    log_(R, Lvl, Fmt, Args);
log(_, _, _, _) ->
    ok.

log_(R, Lvl, Fmt, Args) ->
    rvi_log:log(R, Lvl, <<"authorize">>, rvi_log:format(Fmt, Args)).
