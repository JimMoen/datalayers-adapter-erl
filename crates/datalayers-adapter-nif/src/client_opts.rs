use rustler::{Decoder, Error, MapIterator, NifResult, Term};
use std::time::Duration;

/// Per-stage flight timeout used when the connect options do not set one.
pub const DEFAULT_TIMEOUT_MS: u64 = 15_000;

/// Whole-request deadline used when the connect options do not set one.
///
/// A request may span several stages (`prepare` + `execute` + `do_get`, plus a
/// statement rebuild), so the per-stage timeout alone bounds it only loosely.
/// This bounds the total time a request can occupy (or wait for) a slot.
///
/// The caller does not have to provide it: the default matches the per-stage
/// default and stays well below EMQX's own `request_ttl` default (45s), so a
/// reply always reaches the buffer worker before it considers the request
/// expired (an expired request's reply is dropped as a late reply).
pub const DEFAULT_REQUEST_TIMEOUT_MS: u64 = 15_000;

/// Process-wide cap on concurrently running requests, used when the connect
/// options do not set one.
///
/// It is a safety net, sized to sit above the pool sizes a node normally runs
/// (EMQX pools default to a handful of workers; the reference testbed uses 200),
/// so it does not throttle healthy traffic while still bounding a stalled server.
pub const DEFAULT_MAX_IN_FLIGHT: usize = 1024;

#[derive(Clone, Debug)]
/// The connect Options for the client connecting to the Datalayers server via Arrow Flight SQL protocol.
pub struct ClientOpts {
    /// The hostname of the Datalayers database server.
    pub host: Option<String>,
    /// The port number on which the Datalayers database server is listening.
    pub port: Option<u16>,
    /// The username for authentication when connecting to the database.
    pub username: Option<String>,
    /// The password for authentication when connecting to the database.
    pub password: Option<String>,
    /// The optional TLS certificate for secure connections.
    /// The certificate is self-signed by Datalayers and is used as the pem file by the client to certify itself.
    pub tls_cert: Option<String>,
    /// Per-stage flight timeout in milliseconds, decided by the caller.
    /// Defaults to [`DEFAULT_TIMEOUT_MS`]; `0` is treated as "not set".
    pub timeout_ms: Option<u64>,
    /// Whole-request deadline in milliseconds, decided by the caller.
    /// Defaults to [`DEFAULT_REQUEST_TIMEOUT_MS`]; `0` is treated as "not set".
    pub request_timeout_ms: Option<u64>,
    /// Process-wide cap on concurrently running requests, decided by the caller.
    /// Defaults to [`DEFAULT_MAX_IN_FLIGHT`]; `0` is treated as "not set".
    pub max_in_flight: Option<usize>,
}

impl Default for ClientOpts {
    fn default() -> ClientOpts {
        ClientOpts {
            host: Some("127.0.0.1".to_string()),
            port: Some(8360),
            username: Some("admin".to_string()),
            password: Some("public".to_string()),
            tls_cert: None,
            timeout_ms: None,
            request_timeout_ms: None,
            max_in_flight: None,
        }
    }
}

impl<'a> Decoder<'a> for ClientOpts {
    fn decode(term: Term<'a>) -> NifResult<Self> {
        let mut opts = Self::default();
        for (key, value) in MapIterator::new(term).ok_or(Error::BadArg)? {
            match key.atom_to_string()?.as_ref() {
                "host" => opts.host = Some(value.decode()?),
                "port" => opts.port = Some(value.decode()?),
                "username" => opts.username = Some(value.decode()?),
                "password" => opts.password = Some(value.decode()?),
                "tls_cert" => opts.tls_cert = Some(value.decode()?),
                "timeout" => opts.timeout_ms = Some(value.decode()?),
                "request_timeout" => opts.request_timeout_ms = Some(value.decode()?),
                "max_in_flight" => opts.max_in_flight = Some(value.decode()?),
                _ => (),
            }
        }
        Ok(opts)
    }
}

impl ClientOpts {
    /// The per-stage flight timeout, as decided by whoever built these options.
    pub fn timeout(&self) -> Duration {
        match self.timeout_ms {
            Some(ms) if ms > 0 => Duration::from_millis(ms),
            _ => Duration::from_millis(DEFAULT_TIMEOUT_MS),
        }
    }

    /// The whole-request deadline, as decided by whoever built these options.
    pub fn request_timeout(&self) -> Duration {
        match self.request_timeout_ms {
            Some(ms) if ms > 0 => Duration::from_millis(ms),
            _ => Duration::from_millis(DEFAULT_REQUEST_TIMEOUT_MS),
        }
    }

    /// The process-wide in-flight cap, as decided by whoever built these options.
    pub fn max_in_flight(&self) -> usize {
        match self.max_in_flight {
            Some(n) if n > 0 => n,
            _ => DEFAULT_MAX_IN_FLIGHT,
        }
    }

    pub fn protocol(&self) -> &str {
        if self.tls_cert.is_some() {
            "https"
        } else {
            "http"
        }
    }

    pub fn format_uri(&self) -> String {
        format!(
            "{}://{}:{}",
            self.protocol(),
            self.host.clone().unwrap_or_default(),
            self.port.unwrap_or(8360)
        )
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ClientOpts, DEFAULT_MAX_IN_FLIGHT, DEFAULT_REQUEST_TIMEOUT_MS, DEFAULT_TIMEOUT_MS,
    };
    use std::time::Duration;

    #[test]
    fn timeout_defaults_to_15s_when_unset() {
        assert_eq!(
            ClientOpts::default().timeout(),
            Duration::from_millis(DEFAULT_TIMEOUT_MS)
        );
        assert_eq!(
            ClientOpts {
                timeout_ms: Some(0),
                ..Default::default()
            }
            .timeout(),
            Duration::from_millis(DEFAULT_TIMEOUT_MS)
        );
    }

    #[test]
    fn timeout_is_decided_by_the_caller() {
        let opts = ClientOpts {
            timeout_ms: Some(2_500),
            ..Default::default()
        };
        assert_eq!(opts.timeout(), Duration::from_millis(2_500));
    }

    #[test]
    fn request_timeout_defaults_to_15s_when_unset() {
        assert_eq!(
            ClientOpts::default().request_timeout(),
            Duration::from_millis(DEFAULT_REQUEST_TIMEOUT_MS)
        );
        assert_eq!(
            ClientOpts {
                request_timeout_ms: Some(0),
                ..Default::default()
            }
            .request_timeout(),
            Duration::from_millis(DEFAULT_REQUEST_TIMEOUT_MS)
        );
    }

    #[test]
    fn request_timeout_is_decided_by_the_caller_and_independent_of_the_stage_timeout() {
        let opts = ClientOpts {
            timeout_ms: Some(2_500),
            request_timeout_ms: Some(700),
            ..Default::default()
        };
        assert_eq!(opts.timeout(), Duration::from_millis(2_500));
        assert_eq!(opts.request_timeout(), Duration::from_millis(700));
    }

    #[test]
    fn max_in_flight_defaults_and_is_decided_by_the_caller() {
        assert_eq!(ClientOpts::default().max_in_flight(), DEFAULT_MAX_IN_FLIGHT);
        assert_eq!(
            ClientOpts {
                max_in_flight: Some(0),
                ..Default::default()
            }
            .max_in_flight(),
            DEFAULT_MAX_IN_FLIGHT
        );
        assert_eq!(
            ClientOpts {
                max_in_flight: Some(7),
                ..Default::default()
            }
            .max_in_flight(),
            7
        );
    }
}
