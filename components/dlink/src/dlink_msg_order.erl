-module(dlink_msg_order).

-export([init/0,
	 new_handler/3,
	 handler_done/3]).


init() ->
    ets:new(msg_queue, [ordered_set, public]).

new_handler(#{<<"chan">> := Chan, <<"service_name">> := Svc} = R, F, T)
  when is_function(F, 1) ->
    Seq = ets:update_counter(T, {seq, Svc}, {2, 1}, {{seq,Svc}, 1}),
    Key = {msg, Svc, Chan, Seq},
    ets:insert(T, {Key, R}),
    case ets:next(T, {msg, Svc, Chan, 0}) of
	Key ->
	    spawn_msg_handler(Key, R, F);
	_ ->
	    queued
    end;
new_handler(#{} = R, F, _) ->
    spawn(fun() ->
		  F(R)
	  end).


handler_done({msg, Svc, Chan, _} = Key, F, T) when is_function(F, 1) ->
    Ret = fetch_r(Key, T),
    ets:delete(T, Key),
    case ets:next(T, {msg, Svc, Chan, 0}) of
	{msg, Svc, Id, _} = NextKey ->
	    R = ets:lookup_element(T, NextKey, 2),
	    spawn_msg_handler(NextKey, R, F);
	_ ->
	    ok
    end,
    Ret.

spawn_msg_handler(Key, R, F) ->
    spawn_monitor(fun() ->
			  exit({msg_handler_done, Key, F(R)})
		  end).

fetch_r(Key, T) ->
    case ets:lookup(T, Key) of
	[{_, #{} = R}] ->
	    R;
	_ ->
	    undefined
    end.
