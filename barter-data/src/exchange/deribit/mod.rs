use self::{
    channel::DeribitChannel, market::DeribitMarket, subscription::DeribitSubResponse,
    trade::DeribitTrades,
};
use crate::{
    ExchangeWsStream, NoInitialSnapshots,
    exchange::{Connector, ExchangeSub, PingInterval, StreamSelector},
    instrument::InstrumentData,
    subscriber::{WebSocketSubscriber, validator::WebSocketSubValidator},
    subscription::{
        book::{OrderBooksL1, OrderBooksL2},
        trade::PublicTrades,
    },
    transformer::stateless::StatelessTransformer,
};
use barter_instrument::exchange::ExchangeId;
use barter_integration::{
    error::SocketError,
    protocol::websocket::{WebSocketSerdeParser, WsMessage},
};
use barter_macro::{DeExchange, SerExchange, StreamConnectorMeta};
use derive_more::Display;
use serde_json::json;
use std::time::Duration;
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

/// [`Deribit`] server base url.
///
/// See docs: <https://docs.deribit.com/api-reference/websocket>
pub const BASE_URL_DERIBIT: &str = "wss://www.deribit.com/ws/api/v2/";

/// [`Deribit`] server [`PingInterval`] duration (30 seconds).
///
/// Deribit requires heartbeat messages every 30 seconds.
/// See docs: <https://docs.deribit.com/api-reference/websocket>
pub const PING_INTERVAL_DERIBIT: Duration = Duration::from_secs(30);

/// Convenient type alias for a Deribit [`ExchangeWsStream`] using [`WebSocketSerdeParser`].
pub type DeribitWsStream<Transformer> = ExchangeWsStream<WebSocketSerdeParser, Transformer>;

/// [`Deribit`] exchange.
///
/// See docs: <https://docs.deribit.com/api-reference/websocket>
#[derive(
    Copy,
    Clone,
    Eq,
    PartialEq,
    Ord,
    PartialOrd,
    Hash,
    Debug,
    Default,
    Display,
    DeExchange,
    SerExchange,
    StreamConnectorMeta,
)]
#[connector(exchange = "deribit")]
pub struct Deribit;

impl Connector for Deribit {
    const ID: ExchangeId = ExchangeId::Deribit;
    type Channel = DeribitChannel;
    type Market = DeribitMarket;
    type Subscriber = WebSocketSubscriber;
    type SubValidator = WebSocketSubValidator;
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
                    sub.channel.as_ref(),
                    sub.market.as_ref(),
                    "100ms"
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
