//! Authentication types for Deribit raw feeds.
//!
//! This module provides backward-compatible re-exports for the authentication
//! functionality that is now integrated into the main [`Deribit`](super::Deribit) connector.
//!
//! # Migration Guide
//!
//! ## Old API (still works)
//! ```rust,no_run
//! use barter_data::exchange::deribit::DeribitAuth;
//!
//! // DeribitAuth is now a type alias for Deribit with raw configuration
//! let auth = DeribitAuth::new("client_id", "client_secret");
//! ```
//!
//! ## New API (recommended)
//! ```rust,no_run
//! use barter_data::exchange::deribit::{Deribit, DeribitCredentials};
//!
//! let deribit = Deribit::raw(DeribitCredentials::new("client_id", "client_secret"));
//! ```

use super::{Deribit, DeribitCredentials};

/// Backward-compatible type alias for authenticated Deribit connector.
///
/// This is now equivalent to `Deribit` configured for raw feeds with credentials.
pub type DeribitAuth = Deribit;

impl DeribitAuth {
    /// Create a new authenticated Deribit connector for raw feeds.
    ///
    /// # Example
    /// ```rust
    /// use barter_data::exchange::deribit::DeribitAuth;
    ///
    /// let auth = DeribitAuth::new("my_client_id", "my_client_secret");
    /// ```
    pub fn new(client_id: impl Into<String>, client_secret: impl Into<String>) -> Self {
        Deribit::raw(DeribitCredentials::new(client_id, client_secret))
    }

    /// Create with a custom URL (for testnet).
    ///
    /// Note: Custom URLs are not currently supported in the new API.
    /// This method exists for backward compatibility and ignores the URL parameter.
    #[deprecated(
        since = "0.1.0",
        note = "Custom URLs are not supported. Use Deribit::default() for mainnet."
    )]
    pub fn with_url(
        client_id: impl Into<String>,
        client_secret: impl Into<String>,
        _url: impl Into<String>,
    ) -> Self {
        Deribit::raw(DeribitCredentials::new(client_id, client_secret))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exchange::deribit::DeribitInterval;

    #[test]
    fn test_deribit_auth_backward_compat() {
        let auth = DeribitAuth::new("test_id", "test_secret");
        assert_eq!(auth.default_interval, DeribitInterval::Raw);
        assert!(auth.credentials.is_some());
        let creds = auth.credentials.unwrap();
        assert_eq!(creds.client_id, "test_id");
        assert_eq!(creds.client_secret, "test_secret");
    }
}
