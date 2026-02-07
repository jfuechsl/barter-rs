use crate::Identifier;
use crate::subscription::{
    book::{OrderBooksL1, OrderBooksL2},
    trade::PublicTrades,
};
use serde::{Deserialize, Serialize};

/// Interval suffix for Deribit market data channels.
///
/// Deribit supports different aggregation intervals for market data feeds.
/// See docs: <https://docs.deribit.com/articles/market-data-collection-best-practices>
#[derive(
    Copy, Clone, Default, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Deserialize, Serialize,
)]
pub enum DeribitInterval {
    /// Raw feed - every single event, no batching (requires authentication).
    /// See docs: <https://support.deribit.com/hc/en-us/articles/29592500256669-Market-Data-Collection-Best-Practices>
    Raw,
    /// Aggregated roughly every 100 ms (publicly available).
    #[default]
    HundredMs,
    /// Aggregated roughly every 2 seconds (where supported).
    Agg2,
}

impl AsRef<str> for DeribitInterval {
    fn as_ref(&self) -> &str {
        match self {
            Self::Raw => "raw",
            Self::HundredMs => "100ms",
            Self::Agg2 => "agg2",
        }
    }
}

/// Type that defines how to translate a Barter [`Subscription`] into a
/// [`Deribit`] channel to be subscribed to.
///
/// See docs: <https://docs.deribit.com/subscriptions>
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Deserialize, Serialize)]
pub struct DeribitChannel {
    base: &'static str,
    interval: DeribitInterval,
}

impl DeribitChannel {
    /// [`Deribit`] real-time trades channel.
    ///
    /// See docs: <https://docs.deribit.com/subscriptions/trades>
    pub fn trades(interval: DeribitInterval) -> Self {
        Self {
            base: "trades",
            interval,
        }
    }

    /// [`Deribit`] ticker channel (L1 orderbook data).
    ///
    /// See docs: <https://docs.deribit.com/subscriptions/ticker>
    pub fn ticker(interval: DeribitInterval) -> Self {
        Self {
            base: "ticker",
            interval,
        }
    }

    /// [`Deribit`] orderbook channel (L2 orderbook data).
    ///
    /// See docs: <https://docs.deribit.com/subscriptions/orderbook>
    pub fn book(interval: DeribitInterval) -> Self {
        Self {
            base: "book",
            interval,
        }
    }

    /// [`Deribit`] real-time trades channel with default 100ms interval (backward compatible).
    pub fn trades_100ms() -> Self {
        Self::trades(DeribitInterval::HundredMs)
    }

    /// [`Deribit`] ticker channel with default 100ms interval (backward compatible).
    pub fn ticker_100ms() -> Self {
        Self::ticker(DeribitInterval::HundredMs)
    }

    /// [`Deribit`] orderbook channel with default 100ms interval (backward compatible).
    pub fn book_100ms() -> Self {
        Self::book(DeribitInterval::HundredMs)
    }

    /// Returns the interval for this channel.
    pub fn interval(&self) -> DeribitInterval {
        self.interval
    }

    /// Returns just the base channel name (e.g., "trades").
    pub fn base(&self) -> &'static str {
        self.base
    }
}

impl AsRef<str> for DeribitChannel {
    fn as_ref(&self) -> &str {
        self.base
    }
}

/// Implement [`Identifier`] for each subscription kind to map to the correct Deribit channel.
///
/// The interval is determined by the `Deribit` instance's default_interval field.
impl<Instrument> Identifier<DeribitChannel>
    for crate::subscription::Subscription<super::Deribit, Instrument, PublicTrades>
where
    Instrument: crate::instrument::InstrumentData,
{
    fn id(&self) -> DeribitChannel {
        DeribitChannel::trades(self.exchange.default_interval)
    }
}

impl<Instrument> Identifier<DeribitChannel>
    for crate::subscription::Subscription<super::Deribit, Instrument, OrderBooksL1>
where
    Instrument: crate::instrument::InstrumentData,
{
    fn id(&self) -> DeribitChannel {
        DeribitChannel::ticker(self.exchange.default_interval)
    }
}

impl<Instrument> Identifier<DeribitChannel>
    for crate::subscription::Subscription<super::Deribit, Instrument, OrderBooksL2>
where
    Instrument: crate::instrument::InstrumentData,
{
    fn id(&self) -> DeribitChannel {
        DeribitChannel::book(self.exchange.default_interval)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deribit_channel_as_ref() {
        // as_ref() should return only the base channel name, not the full string.
        // The full string (e.g., "trades.100ms") is built in Connector::requests().
        assert_eq!(DeribitChannel::trades_100ms().as_ref(), "trades");
        assert_eq!(DeribitChannel::ticker_100ms().as_ref(), "ticker");
        assert_eq!(DeribitChannel::book_100ms().as_ref(), "book");
    }

    #[test]
    fn test_deribit_channel_serialize() {
        let channel = DeribitChannel::trades_100ms();
        let json = serde_json::to_string(&channel).unwrap();
        assert_eq!(json, r#"{"base":"trades","interval":"HundredMs"}"#);
    }

    #[test]
    fn test_deribit_interval_as_ref() {
        assert_eq!(DeribitInterval::Raw.as_ref(), "raw");
        assert_eq!(DeribitInterval::HundredMs.as_ref(), "100ms");
        assert_eq!(DeribitInterval::Agg2.as_ref(), "agg2");
    }

    #[test]
    fn test_deribit_channel_interval() {
        let channel = DeribitChannel::trades(DeribitInterval::Raw);
        assert_eq!(channel.interval(), DeribitInterval::Raw);
        assert_eq!(channel.as_ref(), "trades");

        let channel = DeribitChannel::book(DeribitInterval::HundredMs);
        assert_eq!(channel.interval(), DeribitInterval::HundredMs);
        assert_eq!(channel.as_ref(), "book");
    }

    #[test]
    fn test_default_interval() {
        assert_eq!(DeribitInterval::default(), DeribitInterval::HundredMs);
    }

    #[test]
    fn test_subscription_id_matches_parse_deribit_channel() {
        use crate::Identifier;
        use crate::exchange::ExchangeSub;
        use crate::exchange::deribit::market::DeribitMarket;
        use crate::exchange::deribit::message::parse_deribit_channel;
        use barter_integration::subscription::SubscriptionId;

        // Simulate the ExchangeSub ID that the subscriber creates
        let exchange_sub = ExchangeSub {
            channel: DeribitChannel::trades(DeribitInterval::HundredMs),
            market: DeribitMarket("BTC-PERPETUAL".into()),
        };
        let sub_id: SubscriptionId = exchange_sub.id();

        // Simulate what parse_deribit_channel produces from an incoming message
        let parsed_id = parse_deribit_channel("trades.BTC-PERPETUAL.100ms").unwrap();

        // These MUST match for the subscription map lookup to work
        assert_eq!(
            sub_id, parsed_id,
            "ExchangeSub ID must match parsed channel ID"
        );

        // Also test with raw interval
        let exchange_sub_raw = ExchangeSub {
            channel: DeribitChannel::book(DeribitInterval::Raw),
            market: DeribitMarket("ETH-PERPETUAL".into()),
        };
        let sub_id_raw: SubscriptionId = exchange_sub_raw.id();
        let parsed_id_raw = parse_deribit_channel("book.ETH-PERPETUAL.raw").unwrap();
        assert_eq!(sub_id_raw, parsed_id_raw);
    }
}
