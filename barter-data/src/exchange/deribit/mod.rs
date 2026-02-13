use self::{
    market::DeribitMarket, message::DeribitParser, subscription::DeribitSubResponse,
    trade::DeribitTrades,
};
use crate::{
    ExchangeWsStream, NoInitialSnapshots,
    exchange::{Connector, ExchangeSub, PingInterval, StreamSelector},
    instrument::InstrumentData,
    subscriber::WebSocketSubscriber,
    subscription::{
        Map,
        book::{OrderBooksL1, OrderBooksL2},
        trade::PublicTrades,
    },
    transformer::stateless::StatelessTransformer,
};
use barter_instrument::exchange::ExchangeId;
use barter_integration::{
    error::SocketError,
    protocol::websocket::{WebSocket, WsMessage},
};
use barter_macro::{DeExchange, SerExchange, StreamConnectorMeta};
use futures::SinkExt;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::{fmt::Debug, time::Duration};
use tracing::debug;
use url::Url;

/// Defines the type that translates a Barter [`Subscription`](crate::subscription::Subscription)
/// into a [`Deribit`] channel to be subscribed to.
pub mod channel;

/// Defines the type that translates a Barter [`Subscription`](crate::subscription::Subscription)
/// into a [`Deribit`] market that can be subscribed to.
pub mod market;

/// [`Subscription`](crate::subscription::Subscription) response type and response
/// [`Validator`](barter_integration::Validator) for [`Deribit`].
pub mod subscription;

/// [`Deribit`] real-time trades WebSocket message.
pub mod trade;

/// OrderBook types for [`Deribit`].
pub mod book;

/// WebSocket message types and parser for [`Deribit`].
pub mod message;

// Re-export types from the channel module
pub use channel::{DeribitChannel, DeribitInterval};

/// [`Deribit`] server base url.
///
/// See docs: <https://docs.deribit.com/api-reference/websocket>
pub const BASE_URL_DERIBIT: &str = "wss://www.deribit.com/ws/api/v2/";

/// [`Deribit`] server [`PingInterval`] duration (30 seconds).
///
/// Deribit requires heartbeat messages every 30 seconds.
/// See docs: <https://docs.deribit.com/api-reference/websocket>
pub const PING_INTERVAL_DERIBIT: Duration = Duration::from_secs(30);

/// Maximum [`Duration`] to wait for a Deribit authentication response before
/// treating the attempt as failed.
pub const AUTH_TIMEOUT: Duration = Duration::from_secs(10);

/// Convenient type alias for a Deribit [`ExchangeWsStream`] using [`DeribitParser`].
pub type DeribitWsStream<Transformer> = ExchangeWsStream<DeribitParser, Transformer>;

/// API credentials for Deribit authentication.
///
/// Required for accessing raw feeds. See:
/// <https://support.deribit.com/hc/en-us/articles/29592500256669-Market-Data-Collection-Best-Practices>
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Deserialize, Serialize, Hash)]
pub struct DeribitCredentials {
    pub client_id: String,
    pub client_secret: String,
}

impl Debug for DeribitCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DeribitCredentials")
            .field("client_id", &self.client_id)
            .field("client_secret", &"<masked>")
            .finish()
    }
}

impl DeribitCredentials {
    /// Create new credentials from client ID and secret.
    pub fn new(client_id: impl Into<String>, client_secret: impl Into<String>) -> Self {
        Self {
            client_id: client_id.into(),
            client_secret: client_secret.into(),
        }
    }
}

/// [`Deribit`] exchange connector.
///
/// Supports both public aggregated feeds (100ms, agg2) and authenticated raw feeds.
///
/// # Interval Limitation
/// The `default_interval` applies to **all** subscriptions in a single connection batch.
/// Subscribing to the same instrument with different intervals (e.g., `Raw` trades and
/// `HundredMs` order books) in the same `StreamBuilder::subscribe` call is **not supported**
/// and will produce incorrect subscription ID matching. Use separate `subscribe` calls
/// (and therefore separate WebSocket connections) for different intervals.
///
/// **Note:** As of this version, `Deribit` no longer implements `Copy` due to
/// the addition of optional credentials containing `String` fields. Use `Clone`
/// instead where needed.
///
/// # Examples
///
/// ## Public feeds (default, no auth required)
/// ```rust,no_run
/// use barter_data::exchange::deribit::Deribit;
///
/// let deribit = Deribit::default(); // Uses 100ms interval, no auth
/// ```
///
/// ## Raw feeds (requires authentication)
/// ```rust,no_run
/// use barter_data::exchange::deribit::{Deribit, DeribitInterval, DeribitCredentials};
///
/// let deribit = Deribit::raw(
///     DeribitCredentials::new("your_client_id", "your_client_secret")
/// );
/// ```
///
/// See docs: <https://docs.deribit.com/api-reference/websocket>
#[derive(
    Clone,
    Eq,
    PartialEq,
    Ord,
    PartialOrd,
    Debug,
    Default,
    DeExchange,
    StreamConnectorMeta,
    Hash,
    SerExchange,
)]
#[connector(exchange = "deribit")]
pub struct Deribit {
    /// Default interval for market data subscriptions.
    pub default_interval: DeribitInterval,
    /// Optional API credentials for authenticated feeds.
    pub credentials: Option<DeribitCredentials>,
}

impl std::fmt::Display for Deribit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Deribit")
    }
}

impl Deribit {
    /// Create a new Deribit connector with the specified interval.
    ///
    /// # Example
    /// ```rust
    /// use barter_data::exchange::deribit::{Deribit, DeribitInterval};
    ///
    /// let deribit = Deribit::with_interval(DeribitInterval::Agg2);
    /// ```
    pub fn with_interval(interval: DeribitInterval) -> Self {
        Self {
            default_interval: interval,
            credentials: None,
        }
    }

    /// Create a new Deribit connector for raw feeds with authentication.
    ///
    /// Raw feeds require API credentials and provide unaggregated real-time data.
    ///
    /// # Example
    /// ```rust
    /// use barter_data::exchange::deribit::{Deribit, DeribitCredentials};
    ///
    /// let deribit = Deribit::raw(DeribitCredentials::new("id", "secret"));
    /// ```
    pub fn raw(credentials: DeribitCredentials) -> Self {
        Self {
            default_interval: DeribitInterval::Raw,
            credentials: Some(credentials),
        }
    }

    /// Set credentials for authentication.
    pub fn with_credentials(mut self, credentials: DeribitCredentials) -> Self {
        self.credentials = Some(credentials);
        self
    }

    /// Set the default interval.
    pub fn with_default_interval(mut self, interval: DeribitInterval) -> Self {
        self.default_interval = interval;
        self
    }
}

impl Connector for Deribit {
    const ID: ExchangeId = ExchangeId::Deribit;
    type Channel = DeribitChannel;
    type Market = DeribitMarket;
    type Subscriber = WebSocketSubscriber;
    type SubValidator = crate::subscriber::validator::WebSocketSubValidator;
    type SubResponse = DeribitSubResponse;

    fn url() -> Result<Url, SocketError> {
        Url::parse(BASE_URL_DERIBIT).map_err(SocketError::UrlParse)
    }

    fn ping_interval() -> Option<PingInterval> {
        Some(PingInterval {
            interval: tokio::time::interval(PING_INTERVAL_DERIBIT),
            ping: || {
                WsMessage::text(
                    json!({
                        "jsonrpc": "2.0",
                        "id": 9999,
                        "method": "public/test"
                    })
                    .to_string(),
                )
            },
        })
    }

    fn requests(exchange_subs: Vec<ExchangeSub<Self::Channel, Self::Market>>) -> Vec<WsMessage> {
        let channels: Vec<String> = exchange_subs
            .into_iter()
            .map(|sub| {
                format!(
                    "{}.{}.{}",
                    sub.channel.base(),
                    sub.market.as_ref(),
                    sub.channel.interval().as_ref()
                )
            })
            .collect();

        vec![WsMessage::text(
            json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "public/subscribe",
                "params": {
                    "channels": channels
                }
            })
            .to_string(),
        )]
    }

    /// Authenticate with Deribit via the `public/auth` JSON-RPC flow if credentials are present.
    ///
    /// See docs: <https://docs.deribit.com/api-reference/authentication>
    async fn authenticate(&self, websocket: &mut WebSocket) -> Result<(), SocketError> {
        let credentials = match &self.credentials {
            Some(creds) => creds,
            None => return Ok(()),
        };

        debug!("authenticating with Deribit");

        let auth_msg = json!({
            "jsonrpc": "2.0",
            "id": 0,
            "method": "public/auth",
            "params": {
                "grant_type": "client_credentials",
                "client_id": credentials.client_id,
                "client_secret": credentials.client_secret
            }
        });

        websocket
            .send(WsMessage::text(auth_msg.to_string()))
            .await
            .map_err(|e| SocketError::WebSocket(Box::new(e)))?;

        // Await Auth Response - loop until we find a response with id: 0
        tokio::time::timeout(AUTH_TIMEOUT, async {
            loop {
                let response = websocket
                    .next()
                    .await
                    .ok_or_else(|| {
                        SocketError::Subscribe(
                            "WebSocket stream terminated during auth".to_string(),
                        )
                    })?
                    .map_err(|e| SocketError::WebSocket(Box::new(e)))?;

                let response_text = match response {
                    WsMessage::Text(t) => t.to_string(),
                    WsMessage::Binary(b) => String::from_utf8(b.to_vec()).unwrap_or_default(),
                    other => {
                        debug!(
                            message_type = ?other,
                            "ignoring non-text WebSocket message during auth"
                        );
                        continue;
                    }
                };

                let json_resp: serde_json::Value = match serde_json::from_str(&response_text) {
                    Ok(v) => v,
                    Err(e) => {
                        debug!(
                            error = ?e,
                            payload = ?response_text,
                            "ignoring malformed JSON message during auth"
                        );
                        continue;
                    }
                };

                // Check if this is the response to our auth request (id: 0)
                if let Some(id) = json_resp.get("id").and_then(|id| id.as_i64()) {
                    if id == 0 {
                        if !json_resp["error"].is_null() {
                            return Err(SocketError::Subscribe(format!(
                                "Deribit authentication failed: {}",
                                json_resp["error"]
                            )));
                        }

                        debug!("Deribit authentication successful");
                        return Ok(());
                    }
                }

                debug!(?json_resp, "ignoring unrelated JSON message during auth");
            }
        })
        .await
        .map_err(|_| {
            SocketError::Subscribe(format!(
                "Deribit authentication timed out after {:?}",
                AUTH_TIMEOUT
            ))
        })?
    }

    fn expected_responses<InstrumentKey>(_: &Map<InstrumentKey>) -> usize {
        1
    }
}

impl<Instrument> StreamSelector<Instrument, PublicTrades> for Deribit
where
    Instrument: InstrumentData,
{
    type SnapFetcher = NoInitialSnapshots;
    type Stream =
        DeribitWsStream<StatelessTransformer<Self, Instrument::Key, PublicTrades, DeribitTrades>>;
}

impl<Instrument> StreamSelector<Instrument, OrderBooksL1> for Deribit
where
    Instrument: InstrumentData,
{
    type SnapFetcher = NoInitialSnapshots;
    type Stream = DeribitWsStream<
        StatelessTransformer<Self, Instrument::Key, OrderBooksL1, book::l1::DeribitTicker>,
    >;
}

impl<Instrument> StreamSelector<Instrument, OrderBooksL2> for Deribit
where
    Instrument: InstrumentData,
{
    type SnapFetcher = NoInitialSnapshots;
    type Stream = DeribitWsStream<
        StatelessTransformer<Self, Instrument::Key, OrderBooksL2, book::l2::DeribitBookUpdate>,
    >;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deribit_default() {
        let deribit = Deribit::default();
        assert_eq!(deribit.default_interval, DeribitInterval::HundredMs);
        assert!(deribit.credentials.is_none());
    }

    #[test]
    fn test_deribit_with_interval() {
        let deribit = Deribit::with_interval(DeribitInterval::Raw);
        assert_eq!(deribit.default_interval, DeribitInterval::Raw);
        assert!(deribit.credentials.is_none());
    }

    #[test]
    fn test_deribit_raw() {
        let creds = DeribitCredentials::new("test_id", "test_secret");
        let deribit = Deribit::raw(creds.clone());
        assert_eq!(deribit.default_interval, DeribitInterval::Raw);
        assert_eq!(deribit.credentials, Some(creds));
    }

    #[test]
    fn test_deribit_builder_pattern() {
        let deribit = Deribit::with_interval(DeribitInterval::Agg2)
            .with_credentials(DeribitCredentials::new("id", "secret"));

        assert_eq!(deribit.default_interval, DeribitInterval::Agg2);
        assert!(deribit.credentials.is_some());
    }

    #[test]
    fn test_deribit_requests_uses_interval() {
        use crate::exchange::ExchangeSub;
        use crate::exchange::deribit::market::DeribitMarket;

        // Test with 100ms interval
        let subs = vec![ExchangeSub {
            channel: DeribitChannel::trades(DeribitInterval::HundredMs),
            market: DeribitMarket("BTC-PERPETUAL".into()),
        }];
        let requests = Deribit::requests(subs);
        let msg = match &requests[0] {
            WsMessage::Text(text) => text,
            _ => panic!("Expected Text message"),
        };
        assert!(msg.contains("trades.BTC-PERPETUAL.100ms"));

        // Test with raw interval
        let subs = vec![ExchangeSub {
            channel: DeribitChannel::book(DeribitInterval::Raw),
            market: DeribitMarket("ETH-PERPETUAL".into()),
        }];
        let requests = Deribit::requests(subs);
        let msg = match &requests[0] {
            WsMessage::Text(text) => text,
            _ => panic!("Expected Text message"),
        };
        assert!(msg.contains("book.ETH-PERPETUAL.raw"));
    }

    #[test]
    fn test_deribit_credentials_debug() {
        let creds = DeribitCredentials::new("my_id", "my_secret");
        let debug_str = format!("{:?}", creds);
        assert!(debug_str.contains("my_id"));
        assert!(debug_str.contains("<masked>"));
        assert!(!debug_str.contains("my_secret"));
    }

    #[test]
    fn test_deribit_display() {
        let deribit = Deribit::default();
        assert_eq!(format!("{}", deribit), "Deribit");
    }

    #[test]
    fn test_deribit_serde_roundtrip() {
        let deribit = Deribit::default();
        let serialized = serde_json::to_string(&deribit).unwrap();
        assert_eq!(
            serialized, r#""deribit""#,
            "Should serialize as exchange ID string"
        );
        let deserialized: Deribit = serde_json::from_str(&serialized).unwrap();
        assert_eq!(deserialized, Deribit::default());
    }
}
