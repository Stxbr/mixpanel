-module(mixpanel_worker).
-export([
	start_link/0,
	track/4
]).

-behaviour(gen_server).
-export([
	init/1,
	handle_call/3,
	handle_cast/2,
	handle_info/2,
	terminate/2,
	code_change/3
]).

-define(BATCH_SIZE, 50).
-define(REQUEST_TIMEOUT, 2 * 5000).
-define(FLUSH_TIMEOUT, 1000).


-type event()      :: {Name :: atom(), mixpanel:properties(), non_neg_integer()}.
-record(state, {
	timeout  :: undefined | reference(),
	pending  :: [event()]
}).


-spec start_link() -> {ok, pid()}.
start_link() ->
	gen_server:start_link(?MODULE, [], []).

-spec track(pid(), atom(), mixpanel:properties(), erlang:datetime()) -> ok.
track(Pid, EventName, Properties, Timestamp) ->
	gen_server:cast(Pid, {track, {EventName, Properties, Timestamp}}).


%% @hidden
init([]) ->
	{ok, #state{
		timeout = undefined,
		pending = []
	}}.

%% @hidden
handle_call(_Request, _From, State) ->
	{stop, unknown_request, State}.

%% @hidden
handle_cast({track, Event}, #state{pending = Pending} = State) when length(Pending) < (?BATCH_SIZE - 1) ->
	cancel_timeout_i(State#state.timeout),
	{noreply, State#state{
		timeout = schedule_timeout_i(),
		pending = [Event | Pending]
	}};
handle_cast({track, Event}, State) ->
	cancel_timeout_i(State#state.timeout),
	ok = track_i(lists:reverse([Event | State#state.pending])),
	{noreply, State#state{
		timeout = undefined,
		pending = []
	}}.

%% @hidden
handle_info({timeout, Ref, flush}, #state{timeout = Ref} = State) ->
	ok = track_i(lists:reverse(State#state.pending)),
	{noreply, State#state{
		timeout = undefined,
		pending = []
	}};
handle_info(_, State) ->
	{noreply, State}.

%% @hidden
terminate(_, _State) ->
	ok.

%% @hidden
code_change(_Old, State, _Extra) ->
	{ok, State}.


%% @private
schedule_timeout_i() ->
	erlang:start_timer(?FLUSH_TIMEOUT, self(), flush).

%% @private
cancel_timeout_i(undefined) ->
	ok;
cancel_timeout_i(Ref) ->
	_ = erlang:cancel_timer(Ref),
	ok.


%% @private
track_i([]) ->
	ok;
track_i(Events) ->
	{ImportEvents, TrackEvents} = lists:partition(fun(X) -> must_import(X) end, Events),
	case application:get_env(mixpanel, token) of
		undefined ->
			ok;
		{ok, Token} ->
			case track_i(list_to_binary(Token), TrackEvents, track, none) of
				ok ->
					ok;
				{error, Error} ->
					error_logger:error_report([
						{message, "Error while sending track events to Mixpanel"},
						{events, TrackEvents},
						{exception, {error, Error}},
						{stacktrace, erlang:get_stacktrace()}
					]),
					ok
			end,
			case application:get_env(mixpanel, api_key) of
				undefined ->
					ok;
				{ok, ApiKey} ->
					case track_i(list_to_binary(Token), ImportEvents, import, list_to_binary(ApiKey)) of
						ok ->
							ok;
						{error, Error2} ->
							error_logger:error_report([
								{message, "Error while sending import events to Mixpanel"},
								{events, ImportEvents},
								{exception, {error, Error2}},
								{stacktrace, erlang:get_stacktrace()}
							]),
							ok
					end
			end
	end.

%% @private
track_i(Token, Events, RequestType, ApiKey) ->
	Data = base64:encode(jiffy:encode([begin
		{[
			{event, Event},
			{properties, {[
				{time, iso8601:format(Time)},
				{token, Token} |
				Properties
			]}}
		]}
	end || {Event, Properties, Time} <- Events], [force_utf8])),

	Headers = [{<<"content-type">>, <<"x-www-form-urlencoded">>}],
	Payload1 = <<"data=", Data/binary>>,
	{Request, Payload2} = case RequestType of
	    import ->
			{<<"import">>, <<Payload1/binary, "&api_key=", ApiKey/binary>>};
		track ->
			{<<"track">>, Payload1}
	end,
	case request_i(post, <<"https://api.mixpanel.com/", Request/binary, "/">>, Headers, Payload2) of
		{ok, <<"1">>} -> ok;
		{ok, _} -> {error, mixpanel_error};
		Error -> Error
	end.

%% @private
request_i(Method, Url, Headers, Payload) ->
	Ref = erlang:make_ref(),
	Parent = self(),
	Pid = spawn(fun() ->
		Options = [{follow_redirect, true}, {recv_timeout, ?REQUEST_TIMEOUT div 2}],
		Result = case hackney:request(Method, Url, Headers, Payload, Options) of
			{ok, _Status, _Headers, Client} ->
				hackney:body(Client);
			{error, Error} ->
				{error, Error}
		end,
		Parent ! {Ref, Result}
	end),
	receive
		{Ref, Result} ->
			Result
	after
		?REQUEST_TIMEOUT ->
			erlang:exit(Pid, timeout),
			{error, timeout}
	end.

must_import({_Event, _Properties, EventDateTime}) ->
	NowDateTime = calendar:universal_time(),
	NowUTC = datetime_to_utc(NowDateTime),
	EventUTC = datetime_to_utc(EventDateTime),
	NowUTC - EventUTC > 24 * 60 * 60 * 5 - 120.

datetime_to_utc(DateTime) -> 
    calendar:datetime_to_gregorian_seconds(DateTime) - 62167219200.
    %% 62167219200 == calendar:datetime_to_gregorian_seconds({{1970, 1, 1}, {0, 0, 0}})
