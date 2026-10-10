-module(datalayers_sock).

-include("datalayers.hrl").

-behavior(gen_server).

-record(state, {
    client :: client_ref() | undefined,
    opts :: opts() | undefined,
    %% id of the request currently in flight. There is at most one per connection,
    %% which is what preserves the order of the synchronous and asynchronous
    %% requests that share this socket.
    current = undefined :: integer() | undefined,
    %% every outstanding request, queued or in flight: id => {Deliver, Func}.
    %% Deliver is {reply, From} for `sync_command/3` callers and
    %% {callback, Callback} for the `datalayers:async_*` callers.
    pending = #{} :: #{integer() => {deliver(), command()}},
    %% requests waiting to be dispatched
    queue = queue:new() :: queue:queue({integer(), command(), args()})
}).

-type deliver() :: {reply, gen_server:from()} | {callback, callback()}.

-export([
    start/0,
    stop/1,
    sync_command/3
]).

%% gen_server callbacks
-export([
    init/1,
    start_link/0,
    handle_call/3,
    handle_info/2,
    handle_cast/2
]).

%% ================================================================================
%% API

start() ->
    start_link().

stop(Client) when is_pid(Client) ->
    catch gen_server:cast(Client, stop),
    ok.

-spec sync_command(client(), command(), args()) -> any().
sync_command(Client, Command, Args) ->
    %% The NIF bounds every flight stage with a timeout and always replies, even on
    %% failure, so the caller needs no deadline of its own here.
    gen_server:call(Client, ?REQ(Command, Args), infinity).

%% ================================================================================
%% gen_server callbacks
start_link() ->
    gen_server:start_link(?MODULE, [], []).

init([]) ->
    {ok, #state{client = undefined, opts = undefined}}.

%% `current = undefined` means nothing is queued or in flight, so this socket
%% has never been asked to connect before. Without it a second `connect` issued
%% while the first is still in flight would be accepted (the `client` field is
%% only filled in by `deliver/4` once the result arrives), and the second
%% connection would silently replace the first one's channel.
handle_call(
    ?REQ(connect, [Opts]),
    From,
    State = #state{client = undefined, current = undefined}
) ->
    {noreply, enqueue(connect, [Opts], {reply, From}, State#state{opts = Opts})};
handle_call(?REQ(connect, _Args), _From, State) ->
    %% Already connected, or a connect request is still queued or in flight.
    {reply, {error, already_connected}, State};
handle_call(?REQ(_Func, _Args), _From, State = #state{client = ClientRef}) when
    not is_reference(ClientRef)
->
    {reply, {error, not_connected}, State};
handle_call(?REQ(Func, Args), From, State) ->
    %% A synchronous request shares the queue with the asynchronous ones, so it must
    %% not overtake the requests already queued or in flight. It is submitted to the
    %% NIF and answered from `handle_info/2` once it completes; the socket process
    %% stays responsive the whole time.
    {noreply, enqueue(Func, Args, {reply, From}, State)}.

%% handle_info(?ASYNC_REQ{...}) -- enqueue and dispatch at most one at a time.
handle_info(?ASYNC_REQ(Func, Args, Callback), State = #state{client = ClientRef}) when
    is_reference(ClientRef)
->
    {noreply, enqueue(Func, Args, {callback, Callback}, State)};
handle_info(?ASYNC_REQ(_Func, _Args, Callback), State) ->
    reply_callback(Callback, {error, not_connected}),
    {noreply, State};
%% completion message sent by the NIF task
handle_info(
    {datalayers_async_result, Id, Res},
    State = #state{current = Id, pending = Pending}
) when
    is_map_key(Id, Pending)
->
    {noreply, maybe_dispatch(complete(Id, Res, State))};
handle_info({datalayers_async_result, _Id, _Res}, State) ->
    %% stale completion (e.g. after stop/restart); ignore
    {noreply, State};
handle_info(_, State) ->
    %% Ignore other messages
    {noreply, State}.

handle_cast(stop, State = #state{client = ClientRef, pending = Pending}) ->
    case is_reference(ClientRef) of
        true ->
            %% Fire and forget: `stop/1` drops the client (closing its channel) on
            %% the NIF task, without blocking this process.
            _ = ?NIF_MODULE:stop(ClientRef);
        false ->
            ok
    end,
    %% Answer queued and in-flight synchronous callers before stopping, otherwise
    %% they would wait forever on `gen_server:call(..., infinity)`.
    ok = reply_pending_sync(Pending),
    {stop, normal, State#state{
        client = undefined, current = undefined, pending = #{}, queue = queue:new()
    }}.

%% ================================================================================
%% Helpers

enqueue(Func, Args, Deliver, State = #state{pending = Pending0, queue = Queue0}) ->
    %% The correlation id is an integer rather than a make_ref(): it is passed into
    %% the Rust NIF and re-encoded into the completion message from a different
    %% environment (the task's OwnedEnv). An integer decodes and encodes as i64
    %% directly, while a Reference is bound to its environment and would need an
    %% extra OwnedEnv::save/load copy. `unique_integer/1` is monotonically unique
    %% per node and never reused, so it is just as safe as a reference.
    Id = erlang:unique_integer([monotonic, positive]),
    Pending = maps:put(Id, {Deliver, Func}, Pending0),
    Queue = queue:in({Id, Func, Args}, Queue0),
    maybe_dispatch(State#state{pending = Pending, queue = Queue}).

maybe_dispatch(State = #state{current = undefined, queue = Queue}) ->
    case queue:out(Queue) of
        {{value, {Id, Func, Args}}, Queue1} ->
            State1 = State#state{queue = Queue1, current = Id},
            case apply_async_nif(Func, Args, State1#state.client, self(), Id) of
                ok ->
                    State1;
                {error, Reason} ->
                    %% Submission rejected (e.g. a stale resource reference); finish
                    %% the request now so the caller is not left waiting.
                    maybe_dispatch(complete(Id, {error, Reason}, State1))
            end;
        {empty, _} ->
            State
    end;
maybe_dispatch(State) ->
    State.

%% Finish one request: run its Deliver function and free the in-flight slot.
complete(Id, Res, State = #state{pending = Pending0}) ->
    {Deliver, Func} = maps:get(Id, Pending0),
    Pending = maps:remove(Id, Pending0),
    deliver(Func, Deliver, Res, State#state{current = undefined, pending = Pending}).

deliver(connect, {reply, From}, {ok, ClientRef} = Res, State) when is_reference(ClientRef) ->
    _ = gen_server:reply(From, Res),
    State#state{client = ClientRef};
deliver(_Func, {reply, From}, Res, State) ->
    _ = gen_server:reply(From, Res),
    State;
deliver(_Func, {callback, Callback}, Res, State) ->
    reply_callback(Callback, Res),
    State.

reply_pending_sync(Pending) ->
    maps:foreach(
        fun(_Id, {Deliver, _Func}) ->
            case Deliver of
                {reply, From} ->
                    _ = gen_server:reply(From, {error, stopped});
                {callback, _Callback} ->
                    %% Asynchronous callers are released by the socket monitor held
                    %% on the caller side (emqx_resource_buffer_worker).
                    ok
            end
        end,
        Pending
    ),
    ok.

apply_async_nif(connect, [Opts], _ClientRef, ReplyPid, Id) ->
    ?NIF_MODULE:async_connect(ReplyPid, Id, Opts);
apply_async_nif(use_database, [Database], ClientRef, ReplyPid, Id) ->
    ?NIF_MODULE:async_use_database(ReplyPid, Id, ClientRef, Database);
apply_async_nif(execute, [Sql], ClientRef, ReplyPid, Id) ->
    ?NIF_MODULE:async_execute(ReplyPid, Id, ClientRef, Sql);
apply_async_nif(prepare, [Sql, AutoRebuild], ClientRef, ReplyPid, Id) ->
    ?NIF_MODULE:async_prepare(ReplyPid, Id, ClientRef, Sql, AutoRebuild);
apply_async_nif(execute_prepare, [Statement, Params], ClientRef, ReplyPid, Id) ->
    ?NIF_MODULE:async_execute_prepare(ReplyPid, Id, ClientRef, Statement, Params);
apply_async_nif(close_prepared, [Statement], ClientRef, ReplyPid, Id) ->
    ?NIF_MODULE:async_close_prepared(ReplyPid, Id, ClientRef, Statement).

reply_callback({CallbackFun, CallbackArgs}, Res) ->
    _ = erlang:apply(CallbackFun, CallbackArgs ++ [Res]),
    ok.
