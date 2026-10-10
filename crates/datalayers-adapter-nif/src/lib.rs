extern crate core;
extern crate rustler;

mod atoms;
mod client;
mod client_opts;
mod resource;
mod types;
mod util;

use crate::resource::{ClientResource, PreparedStatementResource};
use arrow_array::RecordBatch;
use arrow_flight::sql::client::PreparedStatement;
use atoms::*;
use client::Client;
use client_opts::ClientOpts;
use lazy_static::lazy_static;
use rustler::env::{OwnedEnv, SavedTerm};
use rustler::types::LocalPid;
use rustler::{Encoder, Env, NifResult, Reference, ResourceArc, Term};
use std::sync::OnceLock;
use std::time::Duration;
use tokio::runtime::Runtime;
use tokio::sync::Semaphore;
use tonic::transport::Channel;

fn is_prepared_statement_lost(err: &anyhow::Error) -> bool {
    for cause in err.chain() {
        if let Some(status) = cause.downcast_ref::<tonic::Status>() {
            return status.code() == tonic::Code::NotFound;
        }
    }
    false
}

rustler::init!("datalayers_nif", load = on_load);

lazy_static! {
    // This unwrap is acceptable because if the runtime fails to build, the NIF cannot run.
    static ref RT: Runtime = Runtime::new().unwrap();
}

pub fn on_load(env: Env, _load_info: Term) -> bool {
    let _ = env.register::<ClientResource>();
    let _ = env.register::<PreparedStatementResource>();
    true
}

// =============================================================================
// Asynchronous NIFs
//
// Every NIF below is submit-and-return: the NIF body only decodes its arguments
// and hands the actual flight round-trip to the shared tokio runtime. It never
// blocks the calling BEAM process, so it can never pin a normal *or* a dirty IO
// scheduler thread -- this is what keeps a stalled Datalayers server from
// starving file and log I/O across the whole VM (emqx/emqx#19342).
//
// When the task finishes (or a per-stage timeout inside `Client` fires) the
// result is delivered to the socket process as:
//
//     {datalayers_async_result, Id, Result}
//
// where `Result` is `{ok, Value}` or `{error, Reason}`. `datalayers_sock` keeps
// at most one operation in flight per connection and matches on `Id`.
// =============================================================================

/// The value computed by an asynchronous task and encoded back into the socket
/// process's environment once the task finishes.
enum AsyncPayload {
    Rows(Vec<Vec<String>>),
    Prepared(ResourceArc<PreparedStatementResource>),
    Connected(ResourceArc<ClientResource>),
    UseDatabase,
    Closed,
    Error(String),
}

impl AsyncPayload {
    fn encode<'a>(self, env: Env<'a>) -> Term<'a> {
        match self {
            AsyncPayload::Rows(rows) => (ok(), rows).encode(env),
            AsyncPayload::Prepared(resource) => (ok(), resource).encode(env),
            AsyncPayload::Connected(resource) => (ok(), resource).encode(env),
            AsyncPayload::UseDatabase => (ok(), "database_changed").encode(env),
            AsyncPayload::Closed => (ok(), prepare_closed()).encode(env),
            AsyncPayload::Error(reason) => (error(), reason).encode(env),
        }
    }
}

/// Process-wide limiter for concurrently running requests.
///
/// It bounds the amount of pending work a stalled server can accumulate: at most
/// this many requests run at once, and anything over the cap waits for a slot
/// under its own request deadline (see [`run_bounded`]).
///
/// The capacity is established by the first connection (from
/// [`ClientOpts::max_in_flight`]); it is a safety net meant to sit above normal
/// pool sizes, not a per-connector tuning knob.
static IN_FLIGHT_LIMITER: OnceLock<Semaphore> = OnceLock::new();

/// Resolve the process-wide in-flight limiter, establishing its capacity on the
/// first call (see [`ClientOpts::max_in_flight`]).
///
/// Only `async_connect` calls this; it stores the resolved limiter in the
/// `ClientResource`, and every other NIF reuses that reference. `submit` takes
/// the limiter explicitly for that reason -- it can not silently pin the cap to
/// a default of its own when some future entry point submits work first.
fn in_flight_limiter(capacity: usize) -> &'static Semaphore {
    IN_FLIGHT_LIMITER.get_or_init(|| Semaphore::new(capacity))
}

/// Run `work` under the process-wide in-flight cap and the whole-request deadline.
///
/// The deadline bounds the *entire* request, including the wait for an in-flight
/// slot, so a request can never hold -- or wait for -- a slot for longer than
/// that. The per-stage timeouts inside `Client` (`timed`) still bound each
/// individual await.
async fn run_bounded<F>(deadline: Duration, permits: &Semaphore, work: F) -> AsyncPayload
where
    F: std::future::Future<Output = AsyncPayload>,
{
    let bounded = async {
        match permits.acquire().await {
            // The permit is held for the whole request.
            Ok(_permit) => work.await,
            Err(_) => AsyncPayload::Error("[request] in-flight limiter closed".to_string()),
        }
    };
    match tokio::time::timeout(deadline, bounded).await {
        Ok(payload) => payload,
        Err(_) => AsyncPayload::Error(format!("[request] timed out after {deadline:?}")),
    }
}

/// Submit `work` to the shared runtime and deliver its result to `reply_pid`.
///
/// The NIF entry point returns immediately, so no BEAM scheduler is ever blocked
/// by the flight round-trip. The task is bounded by the in-flight cap and the
/// whole-request deadline before it runs.
fn submit<F>(reply_pid: LocalPid, id: i64, deadline: Duration, permits: &'static Semaphore, work: F)
where
    F: std::future::Future<Output = AsyncPayload> + Send + 'static,
{
    RT.spawn(async move {
        let payload = run_bounded(deadline, permits, work).await;
        let mut env = OwnedEnv::new();
        let _ = env.send_and_clear(&reply_pid, |env| {
            (datalayers_async_result(), id, payload.encode(env)).encode(env)
        });
    });
}

#[rustler::nif]
fn async_connect<'a>(
    env: Env<'a>,
    reply_pid: LocalPid,
    id: i64,
    opts: ClientOpts,
) -> NifResult<Term<'a>> {
    let deadline = opts.request_timeout();
    // The first connection establishes the process-wide in-flight cap.
    let permits = in_flight_limiter(opts.max_in_flight());
    submit(reply_pid, id, deadline, permits, async move {
        match Client::try_new(&opts).await {
            Ok(client) => AsyncPayload::Connected(ClientResource::new(client, deadline, permits)),
            Err(e) => AsyncPayload::Error(format!("{e:#}")),
        }
    });
    Ok(ok().encode(env))
}

#[rustler::nif]
fn async_use_database<'a>(
    env: Env<'a>,
    reply_pid: LocalPid,
    id: i64,
    client_resource_ref: Reference<'a>,
    database: String,
) -> NifResult<Term<'a>> {
    let client_resource: ResourceArc<ClientResource> = match client_resource_ref.decode() {
        Ok(r) => r,
        Err(_) => return Ok((error(), "invalid_client_resource").encode(env)),
    };
    let request_timeout = client_resource.request_timeout;
    let permits = client_resource.permits;

    submit(reply_pid, id, request_timeout, permits, async move {
        let mut client_guard = client_resource.inner.lock().await;
        match &mut *client_guard {
            Some(client) => {
                client.use_database(&database);
                AsyncPayload::UseDatabase
            }
            None => AsyncPayload::Error("client_stopped".to_string()),
        }
    });

    Ok(ok().encode(env))
}

/// execute a SQL query using the client resource.
/// Return the result as a term.
/// example:
/// ```erlang
/// 0> datalayers:execute(Client, <<"SHOW DATABASES">>).
/// {ok,[[<<"information_schema">>, <<"2025-07-08T17:37:16+08:00">>],
///      [<<"rust">>,               <<"2025-06-20T11:15:32+08:00">>]
///     ]} | {error, Reason}
/// ```
#[rustler::nif]
fn async_execute<'a>(
    env: Env<'a>,
    reply_pid: LocalPid,
    id: i64,
    client_resource_ref: Reference<'a>,
    sql: String,
) -> NifResult<Term<'a>> {
    let client_resource: ResourceArc<ClientResource> = match client_resource_ref.decode() {
        Ok(r) => r,
        Err(_) => return Ok((error(), "invalid_client_resource").encode(env)),
    };
    let request_timeout = client_resource.request_timeout;
    let permits = client_resource.permits;

    submit(reply_pid, id, request_timeout, permits, async move {
        let mut client_guard = client_resource.inner.lock().await;
        match &mut *client_guard {
            Some(client) => match client.execute(&sql).await {
                Ok(batches) => AsyncPayload::Rows(util::record_batch_to_term(&batches[..])),
                Err(e) => AsyncPayload::Error(format!("{e:#}")),
            },
            None => AsyncPayload::Error("client_stopped".to_string()),
        }
    });

    Ok(ok().encode(env))
}

#[rustler::nif]
fn async_prepare<'a>(
    env: Env<'a>,
    reply_pid: LocalPid,
    id: i64,
    client_resource_ref: Reference<'a>,
    sql: String,
    auto_rebuild: bool,
) -> NifResult<Term<'a>> {
    let client_resource: ResourceArc<ClientResource> = match client_resource_ref.decode() {
        Ok(r) => r,
        Err(_) => return Ok((error(), "invalid_client_resource").encode(env)),
    };
    let request_timeout = client_resource.request_timeout;
    let permits = client_resource.permits;

    submit(reply_pid, id, request_timeout, permits, async move {
        let mut client_guard = client_resource.inner.lock().await;
        match &mut *client_guard {
            Some(client) => match client.prepare(&sql).await {
                Ok(statement) => AsyncPayload::Prepared(PreparedStatementResource::new(
                    statement,
                    sql,
                    auto_rebuild,
                )),
                Err(e) => AsyncPayload::Error(format!("{e:#}")),
            },
            None => AsyncPayload::Error("client_stopped".to_string()),
        }
    });

    Ok(ok().encode(env))
}

#[rustler::nif]
fn async_execute_prepare<'a>(
    env: Env<'a>,
    reply_pid: LocalPid,
    id: i64,
    client_resource_ref: Reference<'a>,
    statement_resource_ref: Reference<'a>,
    params: Term<'a>,
) -> NifResult<Term<'a>> {
    let client_resource: ResourceArc<ClientResource> = match client_resource_ref.decode() {
        Ok(r) => r,
        Err(_) => return Ok((error(), "invalid_client_resource").encode(env)),
    };
    let statement_resource: ResourceArc<PreparedStatementResource> =
        match statement_resource_ref.decode() {
            Ok(r) => r,
            Err(_) => return Ok((error(), "invalid_statement_resource").encode(env)),
        };
    let request_timeout = client_resource.request_timeout;
    let permits = client_resource.permits;

    // Only the term copy happens on the BEAM scheduler. Locking the resources and
    // converting the parameter matrix into an Arrow `RecordBatch` both scale with
    // the input size, so they happen on the task thread; the parameters travel
    // there through an owned environment.
    let owned_env = OwnedEnv::new();
    let saved_params = owned_env.save(params);

    submit(reply_pid, id, request_timeout, permits, async move {
        let mut client_guard = client_resource.inner.lock().await;
        let Some(client) = &mut *client_guard else {
            return AsyncPayload::Error("client_or_statement_stopped".to_string());
        };
        let mut statement_guard = statement_resource.inner.lock().await;

        // `close_prepared/1` takes the statement out of the resource; rebuild it when
        // the caller asked for auto_rebuild, exactly like the synchronous path does.
        //
        // `&OwnedEnv` is `Send` but not `Sync`, so a borrow of it must never be held
        // across an await or the whole task future stops being `Send`. The parameter
        // binding is therefore built in a synchronous scope, between the awaits.
        if statement_guard.is_none() {
            if !statement_resource.auto_rebuild {
                return AsyncPayload::Error("client_or_statement_stopped".to_string());
            }
            return match client.prepare(&statement_resource.sql).await {
                Ok(mut new_statement) => {
                    let binding =
                        match binding_from_saved(&owned_env, &saved_params, &new_statement) {
                            Ok(binding) => binding,
                            Err(err) => return err,
                        };
                    match client.execute_prepared(&mut new_statement, binding).await {
                        Ok(batches) => {
                            *statement_guard = Some(new_statement);
                            AsyncPayload::Rows(util::record_batch_to_term(&batches[..]))
                        }
                        Err(e2) => AsyncPayload::Error(format!("{e2:#}")),
                    }
                }
                Err(rebuild_err) => AsyncPayload::Error(format!("{rebuild_err:#}")),
            };
        }
        let statement = match &mut *statement_guard {
            Some(statement) => statement,
            None => return AsyncPayload::Error("client_or_statement_stopped".to_string()),
        };

        let binding = match binding_from_saved(&owned_env, &saved_params, statement) {
            Ok(binding) => binding,
            Err(err) => return err,
        };

        match client.execute_prepared(statement, binding).await {
            Ok(batches) => AsyncPayload::Rows(util::record_batch_to_term(&batches[..])),
            Err(e) => {
                if statement_resource.auto_rebuild && is_prepared_statement_lost(&e) {
                    match client.prepare(&statement_resource.sql).await {
                        Ok(mut new_statement) => {
                            let new_binding =
                                match binding_from_saved(&owned_env, &saved_params, &new_statement)
                                {
                                    Ok(binding) => binding,
                                    Err(err) => return err,
                                };
                            match client
                                .execute_prepared(&mut new_statement, new_binding)
                                .await
                            {
                                Ok(batches) => {
                                    *statement = new_statement;
                                    AsyncPayload::Rows(util::record_batch_to_term(&batches[..]))
                                }
                                Err(e2) => AsyncPayload::Error(format!("{e2:#}")),
                            }
                        }
                        Err(rebuild_err) => AsyncPayload::Error(format!("{rebuild_err:#}")),
                    }
                } else {
                    AsyncPayload::Error(format!("{e:#}"))
                }
            }
        }
    });

    Ok(ok().encode(env))
}

#[rustler::nif]
fn async_close_prepared<'a>(
    env: Env<'a>,
    reply_pid: LocalPid,
    id: i64,
    client_resource_ref: Reference<'a>,
    statement_resource_ref: Reference<'a>,
) -> NifResult<Term<'a>> {
    let client_resource: ResourceArc<ClientResource> = match client_resource_ref.decode() {
        Ok(r) => r,
        Err(_) => return Ok((error(), "invalid_client_resource").encode(env)),
    };
    let statement_resource: ResourceArc<PreparedStatementResource> =
        match statement_resource_ref.decode() {
            Ok(r) => r,
            Err(_) => return Ok((error(), "invalid_statement_resource").encode(env)),
        };
    let request_timeout = client_resource.request_timeout;
    let permits = client_resource.permits;

    submit(reply_pid, id, request_timeout, permits, async move {
        let client_guard = client_resource.inner.lock().await;
        let mut statement_guard = statement_resource.inner.lock().await;
        if let (Some(client), Some(statement)) = (&*client_guard, statement_guard.take()) {
            match client.close_prepared(statement).await {
                Ok(_) => AsyncPayload::Closed,
                Err(e) => AsyncPayload::Error(format!("{e:#}")),
            }
        } else {
            AsyncPayload::Error("client_or_statement_stopped".to_string())
        }
    });

    Ok(ok().encode(env))
}

/// Drop the client (closing the underlying channel) without blocking the caller.
///
/// The caller is not waiting for a reply: the value is only used to release the
/// gRPC channel when a socket is stopped.
#[rustler::nif]
fn stop(client_resource_ref: Reference) -> rustler::Atom {
    let client_resource: ResourceArc<ClientResource> = match client_resource_ref.decode() {
        Ok(r) => r,
        Err(_) => return error(),
    };

    RT.spawn(async move {
        let _ = client_resource.inner.lock().await.take();
    });

    ok()
}

/// Build the Arrow binding for `statement` from parameters saved in `owned_env`.
///
/// This runs on the task thread: converting the parameter matrix scales with the
/// input size, so it must not happen on a BEAM scheduler.
fn binding_from_saved(
    owned_env: &OwnedEnv,
    saved_params: &SavedTerm,
    statement: &PreparedStatement<Channel>,
) -> Result<RecordBatch, AsyncPayload> {
    owned_env.run(|env| {
        let params = saved_params.load(env);
        match util::params_to_record_batch(statement, params) {
            Ok(binding) => Ok(binding),
            Err(rustler::Error::BadArg) => Err(AsyncPayload::Error("badarg".to_string())),
            Err(e) => Err(AsyncPayload::Error(format!("invalid_params: {e:?}"))),
        }
    })
}

#[cfg(test)]
mod tests {
    use super::{AsyncPayload, in_flight_limiter, is_prepared_statement_lost, run_bounded};
    use std::time::Duration;
    use tokio::sync::Semaphore;

    #[test]
    fn not_found_is_lost() {
        let err = anyhow::Error::new(tonic::Status::not_found("gone"));
        assert!(is_prepared_statement_lost(&err));
    }

    #[test]
    fn other_status_is_not_lost() {
        let err = anyhow::Error::new(tonic::Status::internal("boom"));
        assert!(!is_prepared_statement_lost(&err));
    }

    #[test]
    fn plain_error_is_not_lost() {
        let err = anyhow::anyhow!("plain");
        assert!(!is_prepared_statement_lost(&err));
    }

    #[test]
    fn wrapped_not_found_is_lost() {
        let err = anyhow::Error::new(tonic::Status::not_found("gone")).context("outer");
        assert!(is_prepared_statement_lost(&err));
    }

    #[tokio::test]
    async fn the_request_deadline_bounds_a_hanging_operation() {
        let permits = Semaphore::new(4);
        let out = run_bounded(
            Duration::from_millis(50),
            &permits,
            std::future::pending::<AsyncPayload>(),
        )
        .await;
        match out {
            AsyncPayload::Error(msg) => {
                assert!(msg.contains("timed out after"), "unexpected error: {msg}");
            }
            _ => panic!("a hanging request must be bounded by the request deadline"),
        }
    }

    #[tokio::test]
    async fn a_fast_operation_is_not_affected_by_the_deadline() {
        let permits = Semaphore::new(4);
        let out = run_bounded(Duration::from_secs(30), &permits, async {
            AsyncPayload::Rows(vec![vec!["ok".to_string()]])
        })
        .await;
        match out {
            AsyncPayload::Rows(rows) => assert_eq!(rows, vec![vec!["ok".to_string()]]),
            _ => panic!("a fast request must run to completion"),
        }
    }

    /// The cap is what bounds pending work: a request that cannot get a slot keeps
    /// waiting until its own deadline, then fails instead of piling up forever.
    #[tokio::test]
    async fn requests_over_the_in_flight_cap_wait_for_their_deadline() {
        let permits = Semaphore::new(1);
        // Hold the only permit, as if another request were in flight.
        let held = permits.acquire().await.expect("acquire the only permit");

        let blocked = run_bounded(Duration::from_millis(50), &permits, async {
            AsyncPayload::Closed
        })
        .await;
        match blocked {
            AsyncPayload::Error(msg) => {
                assert!(msg.contains("timed out after"), "unexpected error: {msg}");
            }
            _ => panic!("a request over the cap must not run while the permit is held"),
        }

        drop(held);
        let allowed = run_bounded(Duration::from_millis(50), &permits, async {
            AsyncPayload::Closed
        })
        .await;
        assert!(matches!(allowed, AsyncPayload::Closed));
    }

    /// The limiter's capacity is established once, by the first caller (the first
    /// connection). `submit` takes the resolved limiter explicitly, so it can not
    /// pin the cap to a default of its own before a connection exists.
    #[test]
    fn the_in_flight_limiter_is_established_once() {
        let first = in_flight_limiter(2);
        let second = in_flight_limiter(64);
        assert!(std::ptr::eq(first, second));
        assert_eq!(second.available_permits(), 2);
    }
}
