-module(mixpanel).
-export([
	track/2,
	track/3,
	start/0,
	stop/0
]).
-export_type([
	properties/0
]).

-type property()   :: {Key :: binary() | atom(), Value :: binary() | atom() | boolean() | integer() | float()}.
-type properties() :: [property()].

start() ->
	ensure_started(mixpanel),
	mixpanel_sup:start_link().

stop() ->
	application:stop(mixpanel).

ensure_started(App) ->
	case application:start(App) of
		ok ->
			true;
		{error, {already_started, App}} ->
			true;
		{error, {not_started, Other}} ->
			ensure_started(Other),
			ensure_started(App)
	end.

-spec track(atom(), properties()) -> ok.
track(Event, Properties) ->
	track(Event, Properties, calendar:universal_time()).

-spec track(atom(), properties(), erlang:datetime()) -> ok.
track(Event, Properties, Timestamp) ->
	mixpanel_worker:track(mixpanel_workers:get(), Event, Properties, Timestamp).
