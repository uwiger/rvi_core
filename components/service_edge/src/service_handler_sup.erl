-module(service_handler_sup).
-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    {ok, { {simple_one_for_one, 5, 10},
	   [
	    {service_handler, {service_handler, start_link, []},
	     temporary, 5000, worker, [service_handler]}
	   ] }}.
