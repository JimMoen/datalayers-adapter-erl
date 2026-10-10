use arrow_flight::sql::client::PreparedStatement;
use rustler::ResourceArc;
use std::time::Duration;
use tokio::sync::{Mutex, Semaphore};
use tonic::transport::Channel;

use crate::client::Client;

/// A resource that holds a client reference for executing SQL queries.
pub struct ClientResource {
    pub inner: Mutex<Option<Client>>,
    /// Whole-request deadline shared by every operation on this connection.
    pub request_timeout: Duration,
    /// Process-wide in-flight limiter, resolved once when the connection is made.
    pub permits: &'static Semaphore,
}

impl rustler::Resource for ClientResource {}

impl ClientResource {
    pub fn new(
        client: Client,
        request_timeout: Duration,
        permits: &'static Semaphore,
    ) -> ResourceArc<Self> {
        ResourceArc::new(Self {
            inner: Mutex::new(Some(client)),
            request_timeout,
            permits,
        })
    }
}

/// A resource that holds a prepared statement for executing SQL queries.
pub struct PreparedStatementResource {
    pub inner: Mutex<Option<PreparedStatement<Channel>>>,
    pub sql: String,
    pub auto_rebuild: bool,
}

impl rustler::Resource for PreparedStatementResource {}

impl PreparedStatementResource {
    pub fn new(
        statement: PreparedStatement<Channel>,
        sql: String,
        auto_rebuild: bool,
    ) -> ResourceArc<Self> {
        ResourceArc::new(Self {
            inner: Mutex::new(Some(statement)),
            sql,
            auto_rebuild,
        })
    }
}
