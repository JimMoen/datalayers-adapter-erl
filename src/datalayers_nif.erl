-module(datalayers_nif).

-export([
    async_connect/3,
    async_use_database/4,
    async_execute/4,
    async_prepare/5,
    async_execute_prepare/5,
    async_close_prepared/4,

    stop/1
]).

-export([init/0]).
-on_load(init/0).

init() ->
    NifName = "libdatalayers_nif",
    Niflib = filename:join(priv_dir(), NifName),
    case erlang:load_nif(Niflib, none) of
        ok ->
            ok;
        {error, _Reason} = Res ->
            Res
    end.

%% =================================================================================================
%% NIFs
%%
%% Every operation is asynchronous: the call returns `ok` as soon as the work has
%% been submitted, or `{error, Reason}` when the arguments could not be accepted
%% (e.g. a stale resource reference). The result is delivered to `ReplyPid` as
%% `{datalayers_async_result, Id, Result}`, where `Result` is `{ok, Value}` or
%% `{error, Reason}`. The NIF never blocks the calling process, so a stalled
%% server can not pin a BEAM scheduler thread.

async_connect(_ReplyPid, _Id, _Opts) ->
    not_loaded(?LINE).

async_use_database(_ReplyPid, _Id, _Client, _Database) ->
    not_loaded(?LINE).

async_execute(_ReplyPid, _Id, _Client, _Sql) ->
    not_loaded(?LINE).

async_prepare(_ReplyPid, _Id, _Client, _Sql, _AutoRebuild) ->
    not_loaded(?LINE).

async_execute_prepare(_ReplyPid, _Id, _Client, _Statement, _Params) ->
    not_loaded(?LINE).

async_close_prepared(_ReplyPid, _Id, _Client, _Statement) ->
    not_loaded(?LINE).

stop(_Client) ->
    not_loaded(?LINE).

%% =================================================================================================
%% Helpers

not_loaded(Line) ->
    erlang:nif_error({error, {not_loaded, [{module, ?MODULE}, {line, Line}]}}).

priv_dir() ->
    case code:priv_dir(?MODULE) of
        {error, _} ->
            EbinDir = filename:dirname(code:which(?MODULE)),
            AppPath = filename:dirname(EbinDir),
            filename:join(AppPath, "priv");
        Path ->
            Path
    end.
