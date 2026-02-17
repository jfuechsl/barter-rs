//! Connector factory for custom exchange configurations
//!
//! This module provides types for supplying custom exchange connector instances
//! to DynamicStreams, enabling authentication and other custom configuration.
//!
//! # Example
//!
//! ```rust,no_run
//! use barter_data::{
//!     exchange::deribit::{Deribit, DeribitCredentials},
//!     exchange::connector_factory::{ConnectorFactory, ExchangeConnector},
//!     streams::builder::dynamic::DynamicStreams,
//! };
//!
//! // Create authenticated Deribit connector for raw feeds
//! let deribit = Deribit::raw(DeribitCredentials::new("client_id", "client_secret"));
//!
//! // Create factory with custom connector
//! let factory = ConnectorFactory::new()
//!     .with_connector(ExchangeConnector::Deribit(deribit));
//!
//! // Use factory when initializing streams
//! // let streams = DynamicStreams::init_with_connectors(subscriptions, &factory).await?;
//! ```

use crate::exchange::{
    binance::{futures::BinanceFuturesUsd, spot::BinanceSpot},
    bitfinex::Bitfinex,
    bitmex::Bitmex,
    bybit::{futures::BybitPerpetualsUsd, spot::BybitSpot},
    coinbase::Coinbase,
    deribit::Deribit,
    gateio::{
        future::{GateioFuturesBtc, GateioFuturesUsd},
        option::GateioOptions,
        perpetual::{GateioPerpetualsBtc, GateioPerpetualsUsd},
        spot::GateioSpot,
    },
    kraken::Kraken,
    okx::Okx,
};
use barter_instrument::exchange::ExchangeId;

/// Pre-configured connector instances that can be passed to DynamicStreams
///
/// This enum contains all exchange connector types and allows providing
/// custom-configured instances for stream initialization. This is particularly
/// useful for exchanges that require authentication (e.g., Deribit raw feeds).
///
/// # Maintenance
///
/// This enum must stay in sync with the connectors listed in
/// [`define_stream_connectors!`](crate::streams::builder::dynamic). Each connector
/// registered in the macro expects a corresponding variant here and an `as_{snake_case}()`
/// accessor method (see [`barter-macro/src/stream_registry.rs`]). Adding a new exchange
/// to the macro without updating this enum will cause a compile error in the generated code.
///
/// # Example
///
/// ```rust
/// use barter_data::{
///     exchange::deribit::{Deribit, DeribitCredentials},
///     exchange::connector_factory::ExchangeConnector,
/// };
///
/// let deribit = Deribit::raw(DeribitCredentials::new("id", "secret"));
/// let connector = ExchangeConnector::Deribit(deribit);
/// ```
#[derive(Clone, Debug)]
pub enum ExchangeConnector {
    /// Binance Spot connector
    BinanceSpot(BinanceSpot),
    /// Binance USD Futures connector
    BinanceFuturesUsd(BinanceFuturesUsd),
    /// Bitfinex connector
    Bitfinex(Bitfinex),
    /// Bitmex connector
    Bitmex(Bitmex),
    /// Bybit Spot connector
    BybitSpot(BybitSpot),
    /// Bybit USD Perpetuals connector
    BybitPerpetualsUsd(BybitPerpetualsUsd),
    /// Coinbase connector
    Coinbase(Coinbase),
    /// Deribit connector
    Deribit(Deribit),
    /// Gate.io Spot connector
    GateioSpot(GateioSpot),
    /// Gate.io USD Futures connector
    GateioFuturesUsd(GateioFuturesUsd),
    /// Gate.io BTC Futures connector
    GateioFuturesBtc(GateioFuturesBtc),
    /// Gate.io BTC Perpetuals connector
    GateioPerpetualsBtc(GateioPerpetualsBtc),
    /// Gate.io USD Perpetuals connector
    GateioPerpetualsUsd(GateioPerpetualsUsd),
    /// Gate.io Options connector
    GateioOptions(GateioOptions),
    /// Kraken connector
    Kraken(Kraken),
    /// OKX connector
    Okx(Okx),
}

impl ExchangeConnector {
    /// Get the ExchangeId for this connector
    ///
    /// # Example
    ///
    /// ```rust
    /// use barter_data::{
    ///     exchange::deribit::Deribit,
    ///     exchange::connector_factory::ExchangeConnector,
    ///     exchange::coinbase::Coinbase,
    /// };
    /// use barter_instrument::exchange::ExchangeId;
    ///
    /// let deribit = ExchangeConnector::Deribit(Deribit::default());
    /// assert_eq!(deribit.exchange_id(), ExchangeId::Deribit);
    ///
    /// let coinbase = ExchangeConnector::Coinbase(Coinbase);
    /// assert_eq!(coinbase.exchange_id(), ExchangeId::Coinbase);
    /// ```
    pub fn exchange_id(&self) -> ExchangeId {
        match self {
            ExchangeConnector::BinanceSpot(_) => ExchangeId::BinanceSpot,
            ExchangeConnector::BinanceFuturesUsd(_) => ExchangeId::BinanceFuturesUsd,
            ExchangeConnector::Bitfinex(_) => ExchangeId::Bitfinex,
            ExchangeConnector::Bitmex(_) => ExchangeId::Bitmex,
            ExchangeConnector::BybitSpot(_) => ExchangeId::BybitSpot,
            ExchangeConnector::BybitPerpetualsUsd(_) => ExchangeId::BybitPerpetualsUsd,
            ExchangeConnector::Coinbase(_) => ExchangeId::Coinbase,
            ExchangeConnector::Deribit(_) => ExchangeId::Deribit,
            ExchangeConnector::GateioSpot(_) => ExchangeId::GateioSpot,
            ExchangeConnector::GateioFuturesUsd(_) => ExchangeId::GateioFuturesUsd,
            ExchangeConnector::GateioFuturesBtc(_) => ExchangeId::GateioFuturesBtc,
            ExchangeConnector::GateioPerpetualsBtc(_) => ExchangeId::GateioPerpetualsBtc,
            ExchangeConnector::GateioPerpetualsUsd(_) => ExchangeId::GateioPerpetualsUsd,
            ExchangeConnector::GateioOptions(_) => ExchangeId::GateioOptions,
            ExchangeConnector::Kraken(_) => ExchangeId::Kraken,
            ExchangeConnector::Okx(_) => ExchangeId::Okx,
        }
    }

    /// Get the BinanceSpot connector if this is one
    pub fn as_binance_spot(&self) -> Option<&BinanceSpot> {
        match self {
            ExchangeConnector::BinanceSpot(c) => Some(c),
            _ => None,
        }
    }

    /// Get the BinanceFuturesUsd connector if this is one
    pub fn as_binance_futures_usd(&self) -> Option<&BinanceFuturesUsd> {
        match self {
            ExchangeConnector::BinanceFuturesUsd(c) => Some(c),
            _ => None,
        }
    }

    /// Get the Bitfinex connector if this is one
    pub fn as_bitfinex(&self) -> Option<&Bitfinex> {
        match self {
            ExchangeConnector::Bitfinex(c) => Some(c),
            _ => None,
        }
    }

    /// Get the Bitmex connector if this is one
    pub fn as_bitmex(&self) -> Option<&Bitmex> {
        match self {
            ExchangeConnector::Bitmex(c) => Some(c),
            _ => None,
        }
    }

    /// Get the BybitSpot connector if this is one
    pub fn as_bybit_spot(&self) -> Option<&BybitSpot> {
        match self {
            ExchangeConnector::BybitSpot(c) => Some(c),
            _ => None,
        }
    }

    /// Get the BybitPerpetualsUsd connector if this is one
    pub fn as_bybit_perpetuals_usd(&self) -> Option<&BybitPerpetualsUsd> {
        match self {
            ExchangeConnector::BybitPerpetualsUsd(c) => Some(c),
            _ => None,
        }
    }

    /// Get the Coinbase connector if this is one
    pub fn as_coinbase(&self) -> Option<&Coinbase> {
        match self {
            ExchangeConnector::Coinbase(c) => Some(c),
            _ => None,
        }
    }

    /// Get the Deribit connector if this is one
    pub fn as_deribit(&self) -> Option<&Deribit> {
        match self {
            ExchangeConnector::Deribit(c) => Some(c),
            _ => None,
        }
    }

    /// Get the GateioSpot connector if this is one
    pub fn as_gateio_spot(&self) -> Option<&GateioSpot> {
        match self {
            ExchangeConnector::GateioSpot(c) => Some(c),
            _ => None,
        }
    }

    /// Get the GateioFuturesUsd connector if this is one
    pub fn as_gateio_futures_usd(&self) -> Option<&GateioFuturesUsd> {
        match self {
            ExchangeConnector::GateioFuturesUsd(c) => Some(c),
            _ => None,
        }
    }

    /// Get the GateioFuturesBtc connector if this is one
    pub fn as_gateio_futures_btc(&self) -> Option<&GateioFuturesBtc> {
        match self {
            ExchangeConnector::GateioFuturesBtc(c) => Some(c),
            _ => None,
        }
    }

    /// Get the GateioPerpetualsBtc connector if this is one
    pub fn as_gateio_perpetuals_btc(&self) -> Option<&GateioPerpetualsBtc> {
        match self {
            ExchangeConnector::GateioPerpetualsBtc(c) => Some(c),
            _ => None,
        }
    }

    /// Get the GateioPerpetualsUsd connector if this is one
    pub fn as_gateio_perpetuals_usd(&self) -> Option<&GateioPerpetualsUsd> {
        match self {
            ExchangeConnector::GateioPerpetualsUsd(c) => Some(c),
            _ => None,
        }
    }

    /// Get the GateioOptions connector if this is one
    pub fn as_gateio_options(&self) -> Option<&GateioOptions> {
        match self {
            ExchangeConnector::GateioOptions(c) => Some(c),
            _ => None,
        }
    }

    /// Get the Kraken connector if this is one
    pub fn as_kraken(&self) -> Option<&Kraken> {
        match self {
            ExchangeConnector::Kraken(c) => Some(c),
            _ => None,
        }
    }

    /// Get the Okx connector if this is one
    pub fn as_okx(&self) -> Option<&Okx> {
        match self {
            ExchangeConnector::Okx(c) => Some(c),
            _ => None,
        }
    }
}

/// Factory for providing custom connector instances to DynamicStreams
///
/// This factory allows users to provide pre-configured exchange connectors
/// (e.g., authenticated instances) when initializing market data streams.
/// Exchanges not configured in the factory will use their default connector.
///
/// # Example
///
/// ```rust
/// use barter_data::{
///     exchange::deribit::{Deribit, DeribitCredentials},
///     exchange::connector_factory::{ConnectorFactory, ExchangeConnector},
/// };
/// use barter_instrument::exchange::ExchangeId;
///
/// // Create factory with authenticated Deribit for raw feeds
/// let factory = ConnectorFactory::new()
///     .with_connector(ExchangeConnector::Deribit(
///         Deribit::raw(DeribitCredentials::new("id", "secret"))
///     ));
///
/// // Retrieve the connector
/// if let Some(ExchangeConnector::Deribit(deribit)) = factory.get(ExchangeId::Deribit) {
///     println!("Found custom Deribit connector");
/// }
/// ```
#[derive(Clone, Debug, Default)]
pub struct ConnectorFactory {
    connectors: Vec<ExchangeConnector>,
}

impl ConnectorFactory {
    /// Create a new empty factory
    ///
    /// # Example
    ///
    /// ```rust
    /// use barter_data::exchange::connector_factory::ConnectorFactory;
    ///
    /// let factory = ConnectorFactory::new();
    /// assert!(factory.is_empty());
    /// ```
    pub fn new() -> Self {
        Self {
            connectors: Vec::new(),
        }
    }

    /// Add a custom connector instance to the factory
    ///
    /// # Example
    ///
    /// ```rust
    /// use barter_data::{
    ///     exchange::deribit::Deribit,
    ///     exchange::connector_factory::{ConnectorFactory, ExchangeConnector},
    /// };
    ///
    /// let factory = ConnectorFactory::new()
    ///     .with_connector(ExchangeConnector::Deribit(Deribit::default()));
    ///
    /// assert_eq!(factory.len(), 1);
    /// ```
    pub fn with_connector(mut self, connector: ExchangeConnector) -> Self {
        // Remove any existing connector for the same exchange
        self.connectors
            .retain(|c| c.exchange_id() != connector.exchange_id());
        self.connectors.push(connector);
        self
    }

    /// Get a connector for the given exchange ID
    ///
    /// Returns `Some(&ExchangeConnector)` if a custom connector is configured
    /// for the exchange, or `None` if the default connector should be used.
    ///
    /// # Example
    ///
    /// ```rust
    /// use barter_data::{
    ///     exchange::deribit::Deribit,
    ///     exchange::coinbase::Coinbase,
    ///     exchange::connector_factory::{ConnectorFactory, ExchangeConnector},
    /// };
    /// use barter_instrument::exchange::ExchangeId;
    ///
    /// let factory = ConnectorFactory::new()
    ///     .with_connector(ExchangeConnector::Deribit(Deribit::default()));
    ///
    /// assert!(factory.get(ExchangeId::Deribit).is_some());
    /// assert!(factory.get(ExchangeId::Coinbase).is_none());
    /// ```
    pub fn get(&self, exchange: ExchangeId) -> Option<&ExchangeConnector> {
        self.connectors.iter().find(|c| c.exchange_id() == exchange)
    }

    /// Returns true if the factory has no connectors
    pub fn is_empty(&self) -> bool {
        self.connectors.is_empty()
    }

    /// Returns the number of connectors in the factory
    pub fn len(&self) -> usize {
        self.connectors.len()
    }

    /// Clear all connectors from the factory
    pub fn clear(&mut self) {
        self.connectors.clear();
    }

    /// Remove a connector for a specific exchange
    ///
    /// Returns true if a connector was removed
    pub fn remove(&mut self, exchange: ExchangeId) -> bool {
        let len_before = self.connectors.len();
        self.connectors.retain(|c| c.exchange_id() != exchange);
        self.connectors.len() < len_before
    }
}

impl FromIterator<ExchangeConnector> for ConnectorFactory {
    fn from_iter<I: IntoIterator<Item = ExchangeConnector>>(iter: I) -> Self {
        let connectors: Vec<_> = iter.into_iter().collect();
        Self { connectors }
    }
}

impl Extend<ExchangeConnector> for ConnectorFactory {
    fn extend<T: IntoIterator<Item = ExchangeConnector>>(&mut self, iter: T) {
        for connector in iter {
            // Remove any existing connector for the same exchange
            self.connectors
                .retain(|c| c.exchange_id() != connector.exchange_id());
            self.connectors.push(connector);
        }
    }
}

impl IntoIterator for ConnectorFactory {
    type Item = ExchangeConnector;
    type IntoIter = std::vec::IntoIter<ExchangeConnector>;

    fn into_iter(self) -> Self::IntoIter {
        self.connectors.into_iter()
    }
}

impl<'a> IntoIterator for &'a ConnectorFactory {
    type Item = &'a ExchangeConnector;
    type IntoIter = std::slice::Iter<'a, ExchangeConnector>;

    fn into_iter(self) -> Self::IntoIter {
        self.connectors.iter()
    }
}

impl ConnectorFactory {
    /// Returns an iterator over references to the connectors in the factory
    pub fn iter(&self) -> std::slice::Iter<'_, ExchangeConnector> {
        self.connectors.iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exchange::deribit::{Deribit, DeribitInterval};

    #[test]
    fn test_connector_factory_empty() {
        let factory = ConnectorFactory::new();
        assert!(factory.is_empty());
        assert_eq!(factory.len(), 0);
        assert!(factory.get(ExchangeId::Deribit).is_none());
        assert!(factory.get(ExchangeId::Coinbase).is_none());
    }

    #[test]
    fn test_connector_factory_default() {
        let factory: ConnectorFactory = Default::default();
        assert!(factory.is_empty());
    }

    #[test]
    fn test_connector_factory_with_deribit() {
        let deribit = Deribit::with_interval(DeribitInterval::Raw);
        let factory = ConnectorFactory::new().with_connector(ExchangeConnector::Deribit(deribit));

        assert!(!factory.is_empty());
        assert_eq!(factory.len(), 1);

        match factory.get(ExchangeId::Deribit) {
            Some(ExchangeConnector::Deribit(conn)) => {
                assert_eq!(conn.default_interval, DeribitInterval::Raw);
            }
            _ => panic!("Expected Deribit connector"),
        }
    }

    #[test]
    fn test_connector_factory_exchange_id() {
        let deribit = Deribit::default();
        let conn = ExchangeConnector::Deribit(deribit);
        assert_eq!(conn.exchange_id(), ExchangeId::Deribit);

        let coinbase = ExchangeConnector::Coinbase(Coinbase);
        assert_eq!(coinbase.exchange_id(), ExchangeId::Coinbase);
    }

    #[test]
    fn test_connector_factory_multiple() {
        let factory = ConnectorFactory::new()
            .with_connector(ExchangeConnector::Deribit(Deribit::default()))
            .with_connector(ExchangeConnector::Coinbase(Coinbase));

        assert_eq!(factory.len(), 2);
        assert!(factory.get(ExchangeId::Deribit).is_some());
        assert!(factory.get(ExchangeId::Coinbase).is_some());
        assert!(factory.get(ExchangeId::BinanceSpot).is_none());
    }

    #[test]
    fn test_connector_factory_duplicate_replaces() {
        let factory = ConnectorFactory::new()
            .with_connector(ExchangeConnector::Deribit(Deribit::with_interval(
                DeribitInterval::Raw,
            )))
            .with_connector(ExchangeConnector::Deribit(Deribit::with_interval(
                DeribitInterval::Agg2,
            )));

        // Should only have one Deribit connector (the second one)
        assert_eq!(factory.len(), 1);

        match factory.get(ExchangeId::Deribit) {
            Some(ExchangeConnector::Deribit(conn)) => {
                assert_eq!(conn.default_interval, DeribitInterval::Agg2);
            }
            _ => panic!("Expected Agg2 interval Deribit connector"),
        }
    }

    #[test]
    fn test_exchange_connector_as_methods() {
        let deribit = ExchangeConnector::Deribit(Deribit::default());
        assert!(deribit.as_deribit().is_some());
        assert!(deribit.as_coinbase().is_none());

        let coinbase = ExchangeConnector::Coinbase(Coinbase);
        assert!(coinbase.as_coinbase().is_some());
        assert!(coinbase.as_deribit().is_none());
    }

    #[test]
    fn test_connector_factory_remove() {
        let mut factory =
            ConnectorFactory::new().with_connector(ExchangeConnector::Deribit(Deribit::default()));

        assert_eq!(factory.len(), 1);
        assert!(factory.remove(ExchangeId::Deribit));
        assert!(factory.is_empty());
        assert!(!factory.remove(ExchangeId::Deribit));
    }

    #[test]
    fn test_connector_factory_clear() {
        let mut factory = ConnectorFactory::new()
            .with_connector(ExchangeConnector::Deribit(Deribit::default()))
            .with_connector(ExchangeConnector::Coinbase(Coinbase));

        assert_eq!(factory.len(), 2);
        factory.clear();
        assert!(factory.is_empty());
    }

    #[test]
    fn test_connector_factory_from_iterator() {
        let connectors = vec![
            ExchangeConnector::Deribit(Deribit::default()),
            ExchangeConnector::Coinbase(Coinbase),
        ];

        let factory: ConnectorFactory = connectors.into_iter().collect();
        assert_eq!(factory.len(), 2);
    }

    #[test]
    fn test_connector_factory_extend() {
        let mut factory =
            ConnectorFactory::new().with_connector(ExchangeConnector::Deribit(Deribit::default()));

        let more = vec![
            ExchangeConnector::Coinbase(Coinbase),
            ExchangeConnector::Kraken(Kraken),
        ];

        factory.extend(more);
        assert_eq!(factory.len(), 3);
    }

    #[test]
    fn test_into_iterator() {
        let factory = ConnectorFactory::new()
            .with_connector(ExchangeConnector::Deribit(Deribit::default()))
            .with_connector(ExchangeConnector::Coinbase(Coinbase));

        let count = factory.iter().count();
        assert_eq!(count, 2);

        let count = factory.into_iter().count();
        assert_eq!(count, 2);
    }
}
