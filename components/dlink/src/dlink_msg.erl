-module(dlink_msg).

-export([validate/3]).

valid_methods(service_edge) ->
    [register_service,
     unregister_service,
     message,
     get_available_services,
     get_node_service_prefix];
valid_methods(data_link) ->
    [service_available,
     service_unavailable,
     handle_remote_message].


args(register_service, _) ->
    [{m, <<"service">>},
     {m, <<"network_address">>},
     {o, <<"opts">>, []}
     | gen_args()];
args(unregister_service, _) ->
    [{m, <<"service">>}
     | gen_args()];
args(message, _) ->
    [{m, <<"service_name">>},
     {m, <<"timeout">>},
     {m, <<"parameters">>},
     {o, <<"files">>},
     {o, <<"reliable">>},
     {o, <<"synch">>},
     {o, <<"max_msg_size">>},
     {o, <<"id">>}
     | gen_args()];
args(get_available_services, _) ->
    gen_args();
args(get_node_service_prefix, _) ->
    [{o, <<"full">>} | gen_args()];
args(service_available, _) ->
    [{m, <<"service_name">>},
     {m, <<"datalink_module">>} | gen_args()];
args(service_unavailable, C) ->
    args(service_available, C);
args(handle_remote_message, C) ->
    [{m, <<"ip">>},
     {m, <<"port">>} | args(message, C)].

gen_args() ->
    [{o, <<"log_id">>}].

%% Individual arg validation. Should actually do something here...
valid_arg(<<"service">>, Svc, M) when M == register_service;
				      M == unregister_service ->
    valid_service_name(Svc);
valid_arg(<<"service_name">>, Svc, message) ->
    valid_service_name(Svc);
valid_arg(_K, V, _Msg) ->
    {ok, V}.

valid_service_name(Svc) ->
    nomatch == binary:match(Svc, [<<":">>, <<"+">>]).

%% validation logic

validate(Component, M, Args) ->
    case valid_method(M, Component) of
	{error,_} = Err -> Err;
	M1              -> validate_(M1, Component, Args)
    end.

valid_method(M0, Component) ->
    M = to_atom(M0),
    case lists:member(M, valid_methods(Component)) of
	true  -> M;
	false -> {error, invalid_method}
    end.

to_atom(A) when is_atom(A) ->
    A;
to_atom(B) when is_binary(B) ->
    binary_to_existing_atom(B, latin1).

validate_(M, Component, Args) ->
    valid_args(M, Args, args(M, Component)).

valid_args(Msg, Args, Expected) when is_list(Args) ->
    valid_args(Expected, Args, [], [], [], Msg);
valid_args(Msg, Args, Expected) when is_map(Args) ->
    %% assume map args are good; remove if not
    valid_args(Expected, Args, Args, [], [], Msg).

valid_args([{m, K}|T], Args, Good, Invalid, Missing, Msg) ->
    case find(K, Args) of
	{_, V} ->
	    maybe_valid(K, V, T, Args, Good, Invalid, Missing, Msg);
	false ->
	    valid_args(T, Args, Good, Invalid, [K|Missing], Msg)
    end;
valid_args([{o, K}|T], Args, Good, Invalid, Missing, Msg) ->
    case find(K, Args) of
	{_, V} ->
	    maybe_valid(K, V, T, Args, Good, Invalid, Missing, Msg);
	false ->
	    valid_args(T, Args, Good, Invalid, Missing, Msg)
    end;
valid_args([], _, Good, Invalid, Missing, _) ->
    case {Invalid, Missing} of
	{[], []} ->
	    make_map(Good);
	_ ->
	    {error,
	     [{missing, Missing} || Missing =/= []]
	     ++ [{invalid_arguments, Invalid} || Invalid =/= []]}
    end.

maybe_valid(K, V, T, Args, Good, Invalid, Missing, Msg) ->
    case valid_arg(K, V, Msg) of
	{ok, V1} ->
	    valid_args(T, Args, add_good(K, V1, Good), Invalid, Missing, Msg);
	{error, E} ->
	    valid_args(T, Args, del_bad(K, Good),
		       [{K, V, E}|Invalid], Missing, Msg)
    end.

find(K, Args) when is_map(Args) ->
    case maps:find(K, Args) of
	{ok, Value} -> {K, Value};
	error       -> false
    end;
find(K, Args) when is_list(Args) ->
    lists:keyfind(K, 2, Args).

make_map(L) when is_list(L) ->
    maps:from_list(L);
make_map(M) when is_map(M) ->
    M.

add_good(K, V, Good) when is_list(Good) ->
    [{K, V}|Good];
add_good(_, _, #{} = Good) ->
    Good.

%% store_good(K, V, Good) when is_list(Good) ->
%%     lists:keystore(K, 1, Good, {K, V});
%% store_good(K, V, #{} = Good) ->
%%     Good#{K => V}.

del_bad(_, Good) when is_list(Good) ->
    Good;
del_bad(K, #{} = Good) ->
    maps:remove(K, Good).
