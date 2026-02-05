use crate::Identifier;
use crate::subscription::{
    book::{OrderBooksL1, OrderBooksL2},
    trade::PublicTrades,
};
use serde::Serialize;

/// Type that defines how to translate a Barter [`Subscription`] into a
/// [`Deribit`] channel to be subscribed to.
///
/// See docs: <https://docs.deribit.com/subscriptions>
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Serialize)]
pub struct DeribitChannel(pub &'static str);

impl DeribitChannel {
    /// [`Deribit`] real-time trades channel.
    ///
    /// See docs: <https://docs.deribit.com/subscriptions/trades>
    pub const TRADES: Self = Self("trades");

    /// [`Deribit`] ticker channel (L1 orderbook data).
    ///
    /// See docs: <https://docs.deribit.com/subscriptions/ticker>
    pub const TICKER: Self = Self("ticker");

    /// [`Deribit`] orderbook channel (L2 orderbook data).
    ///
    /// See docs: <https://docs.deribit.com/subscriptions/orderbook>
    pub const BOOK: Self = Self("book");
}

impl AsRef<str> for DeribitChannel {
    fn as_ref(&self) -> &str {
        self.0
    }
}

/// Implement [`Identifier`] for each subscription kind to map to the correct Deribit channel.
impl<Instrument> Identifier<DeribitChannel>
    for crate::subscription::Subscription<super::Deribit, Instrument, PublicTrades>
where
    Instrument: crate::instrument::InstrumentData,
{
    fn id(&self) -> DeribitChannel {
        DeribitChannel::TRADES
    }
}

impl<Instrument> Identifier<DeribitChannel>
    for crate::subscription::Subscription<super::Deribit, Instrument, OrderBooksL1>
where
    Instrument: crate::instrument::InstrumentData,
{
    fn id(&self) -> DeribitChannel {
        DeribitChannel::TICKER
    }
}

impl<Instrument> Identifier<DeribitChannel>
    for crate::subscription::Subscription<super::Deribit, Instrument, OrderBooksL2>
where
    Instrument: crate::instrument::InstrumentData,
{
    fn id(&self) -> DeribitChannel {
        DeribitChannel::BOOK
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deribit_channel_as_ref() {
        assert_eq!(DeribitChannel::TRADES.as_ref(), "trades");
        assert_eq!(DeribitChannel::TICKER.as_ref(), "ticker");
        assert_eq!(DeribitChannel::BOOK.as_ref(), "book");
    }

    #[test]
    fn test_deribit_channel_serialize() {
        let channel = DeribitChannel::TRADES;
        let json = serde_json::to_string(&channel).unwrap();
        assert_eq!(json, "\"trades\"");
    }
}
