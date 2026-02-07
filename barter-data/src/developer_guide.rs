//! # Developer Guide: Extending Barter-Data
//!
//! This guide provides comprehensive documentation for developers looking to extend Barter-Data
//! with new exchange connectors or subscription kinds.
//!
//! ## Table of Contents
//!
//! - [Adding a New Exchange Connector](#adding-a-new-exchange-connector)
//! - [Adding a New Subscription Kind](#adding-a-new-subscription-kind)
//! - [Advanced Topics](#advanced-topics)
//! - [Testing and Best Practices](#testing-and-best-practices)
//!
//! ## Adding a New Exchange Connector
//!
//! This section walks through adding support for a new cryptocurrency exchange to Barter-Data.
//!
//! ### Overview
//!
//! Adding a new exchange involves:
//! 1. Creating the module structure
//! 2. Implementing the [`Connector`](crate::exchange::Connector) trait
//! 3. Defining exchange-specific types (Channel, Market, etc.)
//! 4. Implementing [`StreamSelector`](crate::exchange::StreamSelector) for supported subscription kinds
//! 5. Creating message types and transformers
//! 6. Adding tests and examples
//!
//! ### Step 1: Create Module Structure
//!
//! Create a new directory under `src/exchange/` with the following structure:
//!
//! ```text
//! src/exchange/my_exchange/
//! ├── mod.rs           # Main connector implementation
//! ├── channel.rs       # Channel type for WebSocket channels
//! ├── market.rs        # Market type for instrument identifiers
//! ├── subscription.rs  # Subscription response validation
//! └── trade.rs         # PublicTrades implementation (example)
//! ```
//!
//! ### Step 2: Implement the Connector Trait
//!
//! In `src/exchange/my_exchange/mod.rs`, implement the [`Connector`](crate::exchange::Connector) trait:
//!
//! ```rust,ignore
//! use crate::exchange::{Connector, ExchangeSub};
//! use barter_instrument::exchange::ExchangeId;
//! use barter_integration::{
//!     error::SocketError,
//!     protocol::websocket::WsMessage,
//! };
//! use serde::{Deserialize, Serialize};
//! use url::Url;
//!
//! // Import your exchange-specific types
//! use self::{
//!     channel::MyExchangeChannel,
//!     market::MyExchangeMarket,
//!     subscription::MyExchangeSubResponse,
//! };
//! use crate::subscriber::{NoAuth, WebSocketSubscriber, validator::WebSocketSubValidator};
//!
//! pub mod channel;
//! pub mod market;
//! pub mod subscription;
//! pub mod trade;
//!
//! /// Base WebSocket URL for MyExchange
//! pub const BASE_URL_MY_EXCHANGE: &str = "wss://api.myexchange.com/ws/v1/market";
//!
//! /// MyExchange connector
//! #[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Default)]
//! pub struct MyExchange;
//!
//! impl Connector for MyExchange {
//!     // Unique identifier - must be added to barter_instrument::exchange::ExchangeId
//!     const ID: ExchangeId = ExchangeId::MyExchange;
//!
//!     // Exchange-specific channel type
//!     type Channel = MyExchangeChannel;
//!
//!     // Exchange-specific market identifier type
//!     type Market = MyExchangeMarket;
//!
//!     // Standard WebSocket subscriber (works for most exchanges)
//!     type Subscriber = WebSocketSubscriber;
//!
//!     // Standard WebSocket subscription validator (works for most exchanges)
//!     type SubValidator = WebSocketSubValidator;
//!
//!     // Exchange-specific subscription response type
//!     type SubResponse = MyExchangeSubResponse;
//!
//!     // No authentication required for this exchange
//!     type Auth = NoAuth;
//!
//!     fn url() -> Result<Url, SocketError> {
//!         Url::parse(BASE_URL_MY_EXCHANGE).map_err(SocketError::UrlParse)
//!     }
//!
//!     fn requests(exchange_subs: Vec<ExchangeSub<Self::Channel, Self::Market>>) -> Vec<WsMessage> {
//!         // Build exchange-specific subscription request payloads
//!         // Example for a JSON-based request format:
//!         vec![WsMessage::text(
//!             serde_json::json!({
//!                 "method": "SUBSCRIBE",
//!                 "params": exchange_subs,
//!                 "id": 1
//!             })
//!             .to_string(),
//!         )]
//!     }
//!
//!     // Optional: Override if exchange requires custom application-level pings
//!     // fn ping_interval() -> Option<PingInterval> {
//!     //     Some(PingInterval {
//!     //         interval: tokio::time::interval(Duration::from_secs(30)),
//!     //         ping: || WsMessage::text("ping"),
//!     //     })
//!     // }
//! }
//! ```
//!
//! #### Important Notes:
//!
//! - **ExchangeId**: You must add your exchange to the `ExchangeId` enum in the `barter_instrument` crate
//! - **Subscriber**: Most exchanges can use [`WebSocketSubscriber`](crate::subscriber::WebSocketSubscriber)
//! - **SubValidator**: Most exchanges can use [`WebSocketSubValidator`](crate::subscriber::validator::WebSocketSubValidator)
//! - **Custom Pings**: Only implement `ping_interval()` if the exchange requires custom application-level pings
//!   beyond the standard WebSocket protocol pings
//!
//! ### Step 3: Define Channel Type
//!
//! In `src/exchange/my_exchange/channel.rs`, define how Barter subscriptions map to
//! exchange-specific channels:
//!
//! ```rust,ignore
//! use crate::{
//!     Identifier,
//!     subscription::{Subscription, trade::PublicTrades, book::{OrderBooksL1, OrderBooksL2}},
//! };
//! use serde::Serialize;
//!
//! /// Type that defines how to translate a Barter [`Subscription`] into a
//! /// MyExchange channel to be subscribed to.
//! #[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Serialize)]
//! pub struct MyExchangeChannel(pub &'static str);
//!
//! impl MyExchangeChannel {
//!     /// Real-time trades channel
//!     pub const TRADES: Self = Self("trade");
//!
//!     /// Level 1 order book channel (best bid/ask)
//!     pub const ORDER_BOOK_L1: Self = Self("ticker");
//!
//!     /// Level 2 order book channel (depth)
//!     pub const ORDER_BOOK_L2: Self = Self("depth");
//! }
//!
//! // Implement Identifier for each supported SubscriptionKind
//! impl<Instrument> Identifier<MyExchangeChannel> for Subscription<MyExchange, Instrument, PublicTrades> {
//!     fn id(&self) -> MyExchangeChannel {
//!         MyExchangeChannel::TRADES
//!     }
//! }
//!
//! impl<Instrument> Identifier<MyExchangeChannel> for Subscription<MyExchange, Instrument, OrderBooksL1> {
//!     fn id(&self) -> MyExchangeChannel {
//!         MyExchangeChannel::ORDER_BOOK_L1
//!     }
//! }
//!
//! impl<Instrument> Identifier<MyExchangeChannel> for Subscription<MyExchange, Instrument, OrderBooksL2> {
//!     fn id(&self) -> MyExchangeChannel {
//!         MyExchangeChannel::ORDER_BOOK_L2
//!     }
//! }
//!
//! impl AsRef<str> for MyExchangeChannel {
//!     fn as_ref(&self) -> &str {
//!         self.0
//!     }
//! }
//! ```
//!
//! ### Step 4: Define Market Type
//!
//! In `src/exchange/my_exchange/market.rs`, define how to convert Barter instruments to
//! exchange-specific market identifiers:
//!
//! ```rust,ignore
//! use crate::{
//!     Identifier,
//!     instrument::InstrumentData,
//!     subscription::{Subscription, trade::PublicTrades},
//! };
//! use serde::Serialize;
//!
//! /// Type that defines how to translate a Barter [`Subscription`] into a
//! /// MyExchange market identifier.
//! #[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Serialize)]
//! pub struct MyExchangeMarket(pub String);
//!
//! // Implement Identifier for each supported SubscriptionKind
//! impl<Instrument> Identifier<MyExchangeMarket> for Subscription<MyExchange, Instrument, PublicTrades>
//! where
//!     Instrument: InstrumentData,
//! {
//!     fn id(&self) -> MyExchangeMarket {
//!         // Convert instrument to exchange format
//!         // Example: BTC/USDT spot -> "BTCUSDT"
//!         MyExchangeMarket(format!(
//!             "{}{}",
//!             self.instrument.base_asset().as_str().to_uppercase(),
//!             self.instrument.quote_asset().as_str().to_uppercase()
//!         ))
//!     }
//! }
//!
//! impl AsRef<str> for MyExchangeMarket {
//!     fn as_ref(&self) -> &str {
//!         &self.0
//!     }
//! }
//! ```
//!
//! ### Step 5: Define Subscription Response Type
//!
//! In `src/exchange/my_exchange/subscription.rs`, define the type used to validate subscription responses:
//!
//! ```rust,ignore
//! use barter_integration::Validator;
//! use serde::{Deserialize, Serialize};
//!
//! /// MyExchange subscription response message
//! #[derive(Clone, Eq, PartialEq, Debug, Deserialize, Serialize)]
//! pub struct MyExchangeSubResponse {
//!     pub result: Option<bool>,
//!     pub id: Option<u64>,
//!     pub error: Option<MyExchangeSubError>,
//! }
//!
//! #[derive(Clone, Eq, PartialEq, Debug, Deserialize, Serialize)]
//! pub struct MyExchangeSubError {
//!     pub code: i64,
//!     pub msg: String,
//! }
//!
//! impl Validator for MyExchangeSubResponse {
//!     fn validate(self) -> Result<Self, barter_integration::error::SocketError> {
//!         if let Some(error) = self.error {
//!             return Err(barter_integration::error::SocketError::Subscribe(format!(
//!                 "MyExchange subscription error {}: {}",
//!                 error.code, error.msg
//!             )));
//!         }
//!
//!         if self.result == Some(true) {
//!             Ok(self)
//!         } else {
//!             Err(barter_integration::error::SocketError::Subscribe(
//!                 "MyExchange subscription failed without error details".to_string(),
//!             ))
//!         }
//!     }
//! }
//! ```
//!
//! ### Step 6: Implement StreamSelector and Message Types
//!
//! For each supported [`SubscriptionKind`](crate::subscription::SubscriptionKind), you need to:
//! 1. Implement [`StreamSelector`](crate::exchange::StreamSelector)
//! 2. Define exchange-specific message types
//! 3. Implement transformation to Barter's normalized types
//!
//! Example for `PublicTrades` in `src/exchange/my_exchange/trade.rs`:
//!
//! ```rust,ignore
//! use crate::{
//!     Identifier,
//!     ExchangeWsStream, NoInitialSnapshots,
//!     event::{MarketEvent, MarketIter},
//!     exchange::{StreamSelector, ExchangeSub},
//!     instrument::InstrumentData,
//!     subscription::trade::{PublicTrades, PublicTrade},
//!     transformer::stateless::StatelessTransformer,
//! };
//! use barter_instrument::{Side, exchange::ExchangeId};
//! use barter_integration::{
//!     protocol::websocket::WebSocketSerdeParser,
//!     subscription::SubscriptionId,
//! };
//! use chrono::{DateTime, Utc};
//! use serde::{Deserialize, Serialize};
//!
//! /// Convenient type alias for MyExchange trades stream
//! pub type MyExchangeTradesStream<InstrumentKey> =
//!     ExchangeWsStream<WebSocketSerdeParser, StatelessTransformer<MyExchange, InstrumentKey, PublicTrades, MyExchangeTrades>>;
//!
//! /// MyExchange message wrapper containing trades data
//! #[derive(Clone, Debug, Deserialize, Serialize)]
//! pub struct MyExchangeTrades {
//!     #[serde(rename = "s")]  // symbol
//!     pub symbol: String,
//!     #[serde(rename = "t")]  // trades
//!     pub trades: Vec<MyExchangeTrade>,
//! }
//!
//! /// Individual trade from MyExchange
//! #[derive(Clone, Debug, Deserialize, Serialize)]
//! pub struct MyExchangeTrade {
//!     #[serde(rename = "i")]
//!     pub id: String,
//!     #[serde(rename = "p", deserialize_with = "barter_integration::de::de_str")]
//!     pub price: f64,
//!     #[serde(rename = "q", deserialize_with = "barter_integration::de::de_str")]
//!     pub quantity: f64,
//!     #[serde(rename = "S")]
//!     pub side: Side,
//!     #[serde(rename = "T", deserialize_with = "barter_integration::de::de_u64_epoch_ms_as_datetime_utc")]
//!     pub time: DateTime<Utc>,
//! }
//!
//! // Implement Identifier to extract SubscriptionId from message
//! impl Identifier<Option<SubscriptionId>> for MyExchangeTrades {
//!     fn id(&self) -> Option<SubscriptionId> {
//!         // Construct SubscriptionId from channel and market
//!         Some(SubscriptionId::from(format!("trade|{}", self.symbol)))
//!     }
//! }
//!
//! // Implement transformation to Barter's normalized PublicTrade events
//! impl<InstrumentKey: Clone> From<(ExchangeId, InstrumentKey, MyExchangeTrades)>
//!     for MarketIter<InstrumentKey, PublicTrade>
//! {
//!     fn from((exchange, instrument, trades): (ExchangeId, InstrumentKey, MyExchangeTrades)) -> Self {
//!         trades
//!             .trades
//!             .into_iter()
//!             .map(|trade| {
//!                 Ok(MarketEvent {
//!                     time_exchange: trade.time,
//!                     time_received: Utc::now(),
//!                     exchange,
//!                     instrument: instrument.clone(),
//!                     kind: PublicTrade {
//!                         id: trade.id,
//!                         price: trade.price,
//!                         amount: trade.quantity,
//!                         side: trade.side,
//!                     },
//!                 })
//!             })
//!             .collect()
//!     }
//! }
//!
//! // Implement StreamSelector to tie everything together
//! impl<Instrument> StreamSelector<Instrument, PublicTrades> for MyExchange
//! where
//!     Instrument: InstrumentData,
//! {
//!     type SnapFetcher = NoInitialSnapshots;
//!     type Stream = MyExchangeTradesStream<Instrument::Key>;
//! }
//! ```
//!
//! ### Step 7: Add Tests
//!
//! Add comprehensive tests for your implementation:
//!
//! ```rust,ignore
//! #[cfg(test)]
//! mod tests {
//!     use super::*;
//!
//!     #[test]
//!     fn test_deserialize_my_exchange_trades() {
//!         let json = r#"{"s":"BTCUSDT","t":[{"i":"12345","p":"50000.0","q":"0.1","S":"buy","T":1609459200000}]}"#;
//!         let trades: MyExchangeTrades = serde_json::from_str(json).unwrap();
//!         assert_eq!(trades.symbol, "BTCUSDT");
//!         assert_eq!(trades.trades.len(), 1);
//!         assert_eq!(trades.trades[0].price, 50000.0);
//!     }
//!
//!     #[test]
//!     fn test_transform_to_barter_event() {
//!         // Test transformation logic
//!     }
//! }
//! ```
//!
//! ### Step 8: Add to Module Exports
//!
//! Add your exchange to `src/exchange/mod.rs`:
//!
//! ```rust,ignore
//! /// `MyExchange` [`Connector`] and [`StreamSelector`] implementations.
//! pub mod my_exchange;
//! ```
//!
//! ## Adding a New Subscription Kind
//!
//! This section describes how to add a new subscription kind (e.g., funding rates, liquidations)
//! that can be used across multiple exchanges.
//!
//! ### Step 1: Define the SubscriptionKind
//!
//! Create a new file `src/subscription/my_kind.rs`:
//!
//! ```rust,ignore
//! use super::SubscriptionKind;
//! use serde::{Deserialize, Serialize};
//!
//! /// Subscription kind for My New Data Kind
//! #[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Default)]
//! pub struct MyKind;
//!
//! impl SubscriptionKind for MyKind {
//!     type Event = MyKindEvent;
//!
//!     fn as_str(&self) -> &'static str {
//!         "my_kind"
//!     }
//! }
//!
//! impl std::fmt::Display for MyKind {
//!     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//!         write!(f, "{}", self.as_str())
//!     }
//! }
//!
//! /// Normalized Barter event for MyKind
//! #[derive(Clone, PartialEq, PartialOrd, Debug, Deserialize, Serialize)]
//! pub struct MyKindEvent {
//!     pub field1: f64,
//!     pub field2: String,
//!     // ... your fields
//! }
//! ```
//!
//! ### Step 2: Add to Subscription Module
//!
//! Add to `src/subscription/mod.rs`:
//!
//! ```rust,ignore
//! /// My new subscription kind
//! pub mod my_kind;
//! ```
//!
//! ### Step 3: Implement for Each Exchange
//!
//! For each exchange that supports this subscription kind, implement [`StreamSelector`](crate::exchange::StreamSelector)
//! and create the necessary message types and transformers (similar to Step 6 in the exchange guide above).
//!
//! ### Step 4: Optional - Add to SubKind Enum
//!
//! If this is a "core" subscription kind that should be available via dynamic streams,
//! add it to the [`SubKind`](crate::subscription::SubKind) enum and update
//! `exchange_supports_instrument_kind_sub_kind` in `src/subscription/mod.rs`.
//!
//! ## Advanced Topics
//!
//! ### Stateful Transformers
//!
//! For subscription kinds that require state (e.g., maintaining an order book),
//! you'll need to implement a custom [`ExchangeTransformer`](crate::transformer::ExchangeTransformer)
//! instead of using [`StatelessTransformer`](crate::transformer::stateless::StatelessTransformer).
//!
//! See examples:
//! - [`BinanceSpotOrderBooksL2Transformer`](crate::exchange::binance::spot::l2::BinanceSpotOrderBooksL2Transformer)
//! - [`BinanceFuturesUsdOrderBooksL2Transformer`](crate::exchange::binance::futures::l2::BinanceFuturesUsdOrderBooksL2Transformer)
//!
//! ### Custom Snapshot Fetchers
//!
//! Some exchanges require fetching an initial snapshot via REST API before streaming updates
//! (common for L2/L3 order books). Implement the [`SnapshotFetcher`](crate::SnapshotFetcher) trait:
//!
//! ```rust,ignore
//! pub struct MyExchangeOrderBookL2SnapshotFetcher;
//!
//! impl SnapshotFetcher<MyExchange, OrderBooksL2> for MyExchangeOrderBookL2SnapshotFetcher {
//!     async fn fetch_snapshots<Instrument>(
//!         subscriptions: &[Subscription<MyExchange, Instrument, OrderBooksL2>],
//!     ) -> Result<Vec<MarketEvent<Instrument::Key, OrderBookL2>>, SocketError>
//!     where
//!         Instrument: InstrumentData,
//!     {
//!         // Fetch initial snapshots via HTTP
//!         // Return as MarketEvents
//!     }
//! }
//! ```
//!
//! ### Handling Multiple Servers
//!
//! Some exchanges have different servers for different instrument kinds (e.g., Binance Spot vs Futures).
//! Use the [`ExchangeServer`](crate::exchange::ExchangeServer) trait:
//!
//! ```rust,ignore
//! #[derive(Default, Debug, Clone)]
//! pub struct MyExchangeServerSpot;
//!
//! impl ExchangeServer for MyExchangeServerSpot {
//!     const ID: ExchangeId = ExchangeId::MyExchangeSpot;
//!     fn websocket_url() -> &'static str {
//!         "wss://spot.myexchange.com/ws"
//!     }
//! }
//!
//! pub type MyExchangeSpot = Binance<MyExchangeServerSpot>;
//! ```
//!
//! ## Testing and Best Practices
//!
//! ### Unit Tests
//!
//! 1. **Deserialization tests**: Test parsing of real exchange messages
//! 2. **Transformation tests**: Verify correct conversion to Barter types
//! 3. **Channel/Market mapping tests**: Ensure subscriptions map correctly
//!
//! ### Integration Tests
//!
//! Create an example in `examples/` directory to test with live data:
//!
//! ```rust,ignore
//! use barter_data::{
//!     exchange::my_exchange::MyExchange,
//!     streams::Streams,
//!     subscription::trade::PublicTrades,
//! };
//! use barter_instrument::instrument::market_data::kind::MarketDataInstrumentKind;
//! use futures::StreamExt;
//!
//! #[tokio::main]
//! async fn main() {
//!     let mut streams = Streams::<PublicTrades>::builder()
//!         .subscribe([(
//!             MyExchange,
//!             "btc",
//!             "usdt",
//!             MarketDataInstrumentKind::Spot,
//!             PublicTrades,
//!         )])
//!         .init()
//!         .await
//!         .unwrap();
//!
//!     let mut joined = streams.select_all();
//!
//!     while let Some(event) = joined.next().await {
//!         println!("{event:?}");
//!     }
//! }
//! ```
//!
//! ### Best Practices
//!
//! 1. **Follow existing patterns**: Look at similar exchanges (e.g., [`Okx`](crate::exchange::okx::Okx))
//!    as reference implementations
//! 2. **Use standard types**: Prefer [`WebSocketSubscriber`](crate::subscriber::WebSocketSubscriber)
//!    and [`StatelessTransformer`](crate::transformer::stateless::StatelessTransformer) when possible
//! 3. **Document thoroughly**: Add doc comments with links to exchange API documentation
//! 4. **Include raw JSON examples**: Document expected message formats in doc comments
//! 5. **Handle errors gracefully**: Return proper error types instead of panicking
//! 6. **Test with real data**: Always test against live exchange data before submitting
//! 7. **Follow Rust conventions**: Run `cargo fmt` and `cargo clippy` before committing
//!
//! ## Examples
//!
//! Comprehensive real-world examples:
//! - Simple exchange: [`Okx`](crate::exchange::okx)
//! - Complex exchange with multiple servers: [`Binance`](crate::exchange::binance)
//! - Stateless subscription: [`PublicTrades`](crate::subscription::trade)
//! - Stateful subscription: [`OrderBooksL2`](crate::subscription::book::OrderBooksL2)
//!
//! ## Common Pitfalls
//!
//! 1. **Forgetting to implement `Identifier` traits**: Both `Channel` and `Market` need `Identifier` impls
//! 2. **Incorrect SubscriptionId construction**: Must match what the exchange sends in messages
//! 3. **Not handling message batches**: Many exchanges send arrays of events in a single message
//! 4. **Timezone issues**: Always convert exchange timestamps to `DateTime<Utc>`
//! 5. **Missing SerDe derives**: Ensure all types derive appropriate `Serialize`/`Deserialize`
//!
//! ## Getting Help
//!
//! - Check the [API Documentation](https://docs.rs/barter-data)
//! - Review existing exchange implementations in `src/exchange/`
//! - Ask on [Discord](https://discord.gg/wE7RqhnQMV)
//! - Open an issue on [GitHub](https://github.com/barter-rs/barter-rs)
