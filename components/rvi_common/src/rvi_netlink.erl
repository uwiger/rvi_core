%% -*- erlang-indent-level: 4; indent-tabs-mode: nil -*-
%%
%% Copyright (C) 2014, Jaguar Land Rover
%%
%% This program is licensed under the terms and conditions of the
%% Mozilla Public License, version 2.0.  The full text of the
%% Mozilla Public License is at https://www.mozilla.org/MPL/2.0/
%%
-module(rvi_netlink).
-behaviour(gen_server).

-export([is_network_up/0, is_network_up/1,
         subscribe/0, subscribe/1, subscribe/2]).

-export([start_link/0]).

%% Gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-include_lib("lager/include/log.hrl").

-record(iface, {name,
                status = down,
                opts = []}).

-record(sub, {name, field, pid, ref}).

-record(st, {connected = false,
             ifs = [],
             subscribers = [],
             poll_ref}).

-define(BADARG, {?MODULE, '__BADARG__'}).
-define(POLL_INTERVAL, 3000).  % we only poll if netlink_drv isn't present

is_network_up() ->
    call(is_network_up).

is_network_up(Iface) ->
    case is_loopback(Iface) of
        true  -> true;
        false ->
            call({is_network_up, Iface})
    end.

subscribe() ->
    subscribe(all, operstate).

subscribe(Iface) ->
    subscribe(Iface, operstate).

subscribe(Iface, Field) ->
    call({subscribe, Iface, Field}).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init(_) ->
    Ref = case code:is_loaded(netlink_drv) of
              false ->
                  %% Must fake event mechanism through polling
                  start_poll_timer();
              {file, _} ->
                  netlink:subscribe("", [operstate], [flush]),
                  undefined
          end,
    Interfaces = get_interfaces(),
    ?debug("get_interfaces() -> ~p", [Interfaces]),
    {ok, conn_status(#st{ifs = Interfaces,
                         poll_ref = Ref})}.

handle_call(is_network_up, _From, #st{connected = Conn} = St) ->
    {reply, Conn, St};
handle_call({is_network_up, Iface}, _From, #st{ifs = Ifs} = S) ->
    {reply, is_network_up_(Iface, Ifs), S};
handle_call({subscribe, Iface, Field}, {Pid, _},
            #st{subscribers = Subs} = St) ->
    Ref = erlang:monitor(process, Pid),
    {reply, ok, St#st{subscribers = [#sub{name = Iface,
                                          field = Field,
                                          pid = Pid,
                                          ref = Ref}|Subs]}};
handle_call(_, _From, St) ->
    {reply, ?BADARG, St}.

handle_cast(_, St) ->
    {noreply, St}.

handle_info({timeout, _Ref, poll}, #st{ifs = Ifs} = S) ->
    NewPollRef = start_poll_timer(),
    NewIfs = get_interfaces(),
    Diffs = lists:foldr(
              fun(#iface{name = Name, status = St}, Acc) ->
                      case lists:keyfind(Name, #iface.name, Ifs) of
                          #iface{status = St0} when St0 =/= St ->
                              [{Name, operstate, St0, St}|Acc];
                          _ ->
                              Acc
                      end
              end, [], NewIfs),
    {noreply, tell_subscribers(Diffs, S#st{ifs = NewIfs,
                                           poll_ref = NewPollRef})};
handle_info({netlink, _NRef, Iface, Field, Prev, New}, St) ->
    {Prev1, New1} = adjust_status(Iface, Field, Prev, New),
    {noreply, tell_subscribers([{Iface, Field, Prev1, New1}], St)};
handle_info(_, St) ->
    {noreply, St}.

terminate(_Reason, _St) ->
    ok.

code_change(_FromVsn, St, _Extra) ->
    {ok, St}.

call(Req) ->
    case gen_server:call(?MODULE, Req) of
        ?BADARG ->
            error(badarg);
        Reply ->
            Reply
    end.

conn_status(#st{ifs = Ifs} = S) ->
    Status = is_network_up_(Ifs),
    ?debug("is_network_up_/1 -> ~p", [Status]),
    S#st{connected = Status}.

tell_subscribers(Evts, #st{subscribers = Subs, ifs = Ifs} = St) ->
    lists:foreach(
      fun({Name, Field, Old, New}) ->
              [Pid ! {rvi_netlink, Ref, Name, Field, Old, New}
               || #sub{name = N, pid = Pid, ref = Ref} <- Subs,
                  match_name(N, Name, Ifs)]
      end, Evts),
    conn_status(St).

get_interfaces() ->
    case inet:getifaddrs() of
        {ok, IFs} ->
            [if_entry(I) || {_Name, _Flags} = I <- IFs];
        Error ->
            ?error("getifaddrs() -> ~p", [Error]),
            []
    end.

is_network_up_([#iface{name = "lo"}|T]) ->
    %% skip loopback (always up)
    is_network_up_(T);
is_network_up_([#iface{status = up}|_]) ->
    true;
is_network_up_([_|T]) ->
    is_network_up_(T);
is_network_up_([]) ->
    false.

is_network_up_(IP, Ifs) when is_tuple(IP) ->
    Opt = {addr, IP},
    case [Iface || #iface{opts = Opts} = Iface <- Ifs,
                   lists:member(Opt, Opts)] of
        [] -> false;
        Matching ->
            is_network_up_(Matching)
    end;
is_network_up_(Iface, Ifs) ->
    case lists:keyfind(str(Iface), #iface.name, Ifs) of
        #iface{status = Status} ->
            Status == up;
        false ->
            false
    end.

if_entry({Name, Opts}) ->
    #iface{name = Name,
           status = if_status(Opts),
           opts = Opts}.

if_status(Opts) ->
    case lists:member(up, opt(flags, Opts, [])) of
        true -> up;
        false -> down
    end.

adjust_status(IF, operstate, A, B) ->
    {adjust_operstate(A, IF), adjust_operstate(B, IF)};
adjust_status(_, _, A, B) ->
    {A, B}.

adjust_operstate(undefined,  _) -> down;
adjust_operstate(unknown, "lo") -> up;
adjust_operstate(State,      _) -> State.

opt(Key, Opts, Default) ->
    case lists:keyfind(Key, 1, Opts) of
        {_, Val} ->
            Val;
        false ->
            Default
    end.

match_name(_, all, _) -> true;
match_name(N, N  , _) -> true;
match_name(A, B  , Ifs) when is_binary(A), is_list(B) ->
    match_name(binary_to_list(A), B, Ifs);
match_name(A, B, _) when is_list(A), is_list(B) ->
    lists:prefix(A, B);
match_name(A, B, Ifs) when is_tuple(A), is_list(B) ->
    case lists:keyfind(B, #iface.name, Ifs) of
        #iface{opts = Opts} ->
            lists:member({addr, A}, Opts);
        _ ->
            false
    end;
match_name(_, _, _) ->
    false.

start_poll_timer() ->
    erlang:start_timer(?POLL_INTERVAL, self(), poll).

str(S) when is_binary(S) ->
    binary_to_list(S);
str(S) when is_list(S) ->
    S.

is_loopback({iface, "lo"})     -> true;
is_loopback("lo")              -> true;
is_loopback({ip, "localhost"}) -> true;
is_loopback({ip, IP})          ->
    case inet:parse_ipv4_address(IP) of
        {ok, {127,0,0,1}} -> true;
        {ok, _}           -> false;
        {error, einval}   ->
            case inet:parse_ipv6_address(IP) of
                {ok, {0,0,0,0,0,0,0,1}} -> true;
                _                       -> false
            end
    end;
is_loopback(_) ->
    false.
