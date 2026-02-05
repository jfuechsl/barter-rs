use crate::{
    Identifier,
    books::Level,
    event::{MarketEvent, MarketIter},
    exchange::{deribit::channel::DeribitChannel, subscription::ExchangeSub},
    subscription::book::OrderBookL1,
};
use barter_instrument::exchange::ExchangeId;
use barter_integration::subscription::SubscriptionId;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

/// [`Deribit`](super::super::Deribit) real-time OrderBook Level1 (ticker) message.
///
/// Deribit's ticker channel provides best bid/ask data suitable for L1 orderbook.
///
/// ### Raw Payload Examples
/// See docs: <https://docs.deribit.com/subscriptions/ticker>
/// #### Ticker (L1)
/// ```json
/// {
///   "jsonrpc": "2.0",
///   "method": "subscription",
///   "params": {
///     "channel": "ticker.BTC-PERPETUAL.100ms",
///     "data": {
///       "timestamp": 1623059681955,
///       "instrument_name": "BTC-PERPETUAL",
///       "best_bid_price": 36289.5,
///       "best_bid_amount": 4600,
///       "best_ask_price": 36290,
///       "best_ask_amount": 53040,
///       "last_price": 36289.5,
///       "mark_price": 36288.31
///     }
///   }
/// }
/// ```
#[derive(Clone, PartialEq, PartialOrd, Debug, Deserialize, Serialize)]
pub struct DeribitTicker {
    #[serde(
        alias = "instrument_name",
        deserialize_with = "de_ticker_subscription_id"
    )]
    pub subscription_id: SubscriptionId,
    #[serde(
        alias = "timestamp",
        deserialize_with = "barter_integration::de::de_u64_epoch_ms_as_datetime_utc"
    )]
    pub time: DateTime<Utc>,
    #[serde(alias = "best_bid_price", deserialize_with = "de_str_to_decimal")]
    pub best_bid_price: Decimal,
    #[serde(alias = "best_bid_amount", deserialize_with = "de_str_to_decimal")]
    pub best_bid_amount: Decimal,
    #[serde(alias = "best_ask_price", deserialize_with = "de_str_to_decimal")]
    pub best_ask_price: Decimal,
    #[serde(alias = "best_ask_amount", deserialize_with = "de_str_to_decimal")]
    pub best_ask_amount: Decimal,
}

/// Deserialize a string field to Decimal (handles both string and numeric JSON).
fn de_str_to_decimal<'de, D>(deserializer: D) -> Result<Decimal, D::Error>
where
    D: serde::de::Deserializer<'de>,
{
    use serde::de::Error;

    // Try as string first (Deribit sometimes sends strings)
    let value: serde_json::Value = Deserialize::deserialize(deserializer)?;
    match value {
        serde_json::Value::String(s) => s.parse::<Decimal>().map_err(D::Error::custom),
        serde_json::Value::Number(n) => {
            Decimal::from_str_exact(&n.to_string()).map_err(D::Error::custom)
        }
        _ => Err(D::Error::custom("expected string or number for decimal")),
    }
}

impl Identifier<Option<SubscriptionId>> for DeribitTicker {
    fn id(&self) -> Option<SubscriptionId> {
        Some(self.subscription_id.clone())
    }
}

impl<InstrumentKey> From<(ExchangeId, InstrumentKey, DeribitTicker)>
    for MarketIter<InstrumentKey, OrderBookL1>
{
    fn from((exchange_id, instrument, ticker): (ExchangeId, InstrumentKey, DeribitTicker)) -> Self {
        let best_ask = if ticker.best_ask_price.is_zero() {
            None
        } else {
            Some(Level::new(ticker.best_ask_price, ticker.best_ask_amount))
        };

        let best_bid = if ticker.best_bid_price.is_zero() {
            None
        } else {
            Some(Level::new(ticker.best_bid_price, ticker.best_bid_amount))
        };

        Self(vec![Ok(MarketEvent {
            time_exchange: ticker.time,
            time_received: Utc::now(),
            exchange: exchange_id,
            instrument,
            kind: OrderBookL1 {
                last_update_time: ticker.time,
                best_bid,
                best_ask,
            },
        })])
    }
}

/// Deserialize a [`DeribitTicker`] "instrument_name" as the associated [`SubscriptionId`].
fn de_ticker_subscription_id<'de, D>(deserializer: D) -> Result<SubscriptionId, D::Error>
where
    D: serde::de::Deserializer<'de>,
{
    <&str as Deserialize>::deserialize(deserializer).map(|market| {
        // Create subscription ID in format: "ticker|instrument_name"
        ExchangeSub::from((DeribitChannel::TICKER, market)).id()
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    mod de {
        use super::*;

        #[test]
        fn test_deribit_ticker_l1() {
            struct TestCase {
                input: &'static str,
                expected: DeribitTicker,
            }

            let time = Utc::now();

            let tests = vec![
                TestCase {
                    // TC0: valid DeribitTicker with numeric prices
                    input: r#"
                    {
                        "timestamp": 1623059681955,
                        "instrument_name": "BTC-PERPETUAL",
                        "best_bid_price": 36289.5,
                        "best_bid_amount": 4600,
                        "best_ask_price": 36290,
                        "best_ask_amount": 53040,
                        "last_price": 36289.5,
                        "mark_price": 36288.31
                    }
                    "#,
                    expected: DeribitTicker {
                        subscription_id: SubscriptionId::from("ticker|BTC-PERPETUAL"),
                        time,
                        best_bid_price: dec!(36289.5),
                        best_bid_amount: dec!(4600),
                        best_ask_price: dec!(36290),
                        best_ask_amount: dec!(53040),
                    },
                },
                TestCase {
                    // TC1: valid DeribitTicker with string prices (some versions send strings)
                    input: r#"
                    {
                        "timestamp": 1623059681955,
                        "instrument_name": "ETH-PERPETUAL",
                        "best_bid_price": "2000.5",
                        "best_bid_amount": "100.5",
                        "best_ask_price": "2001.0",
                        "best_ask_amount": "200.25",
                        "last_price": "2000.75",
                        "mark_price": "2000.5"
                    }
                    "#,
                    expected: DeribitTicker {
                        subscription_id: SubscriptionId::from("ticker|ETH-PERPETUAL"),
                        time,
                        best_bid_price: dec!(2000.5),
                        best_bid_amount: dec!(100.5),
                        best_ask_price: dec!(2001.0),
                        best_ask_amount: dec!(200.25),
                    },
                },
            ];

            for (index, test) in tests.into_iter().enumerate() {
                let actual = serde_json::from_str::<DeribitTicker>(test.input).unwrap();
                let actual = DeribitTicker { time, ..actual };

                assert_eq!(
                    actual.subscription_id, test.expected.subscription_id,
                    "TC{}: subscription_id mismatch",
                    index
                );
                assert_eq!(
                    actual.best_bid_price, test.expected.best_bid_price,
                    "TC{}: best_bid_price mismatch",
                    index
                );
                assert_eq!(
                    actual.best_bid_amount, test.expected.best_bid_amount,
                    "TC{}: best_bid_amount mismatch",
                    index
                );
                assert_eq!(
                    actual.best_ask_price, test.expected.best_ask_price,
                    "TC{}: best_ask_price mismatch",
                    index
                );
                assert_eq!(
                    actual.best_ask_amount, test.expected.best_ask_amount,
                    "TC{}: best_ask_amount mismatch",
                    index
                );
            }
        }

        #[test]
        fn test_deribit_ticker_future() {
            let input = r#"
            {
                "timestamp": 1623059681955,
                "instrument_name": "BTC-27SEP24",
                "best_bid_price": 35000,
                "best_bid_amount": 1000,
                "best_ask_price": 35001,
                "best_ask_amount": 2000,
                "last_price": 35000.5,
                "mark_price": 35000
            }
            "#;

            let actual = serde_json::from_str::<DeribitTicker>(input).unwrap();
            assert_eq!(
                actual.subscription_id,
                SubscriptionId::from("ticker|BTC-27SEP24")
            );
        }
    }

    #[test]
    fn test_from_deribit_ticker_to_market_iter() {
        use barter_instrument::exchange::ExchangeId;

        let time = Utc::now();
        let ticker = DeribitTicker {
            subscription_id: SubscriptionId::from("ticker|BTC-PERPETUAL"),
            time,
            best_bid_price: dec!(36289.5),
            best_bid_amount: dec!(4600),
            best_ask_price: dec!(36290),
            best_ask_amount: dec!(53040),
        };

        let market_iter: MarketIter<String, OrderBookL1> =
            (ExchangeId::Deribit, "BTC-PERPETUAL".to_string(), ticker).into();

        assert_eq!(market_iter.0.len(), 1);
        let event = market_iter.0[0].as_ref().unwrap();
        assert_eq!(event.exchange, ExchangeId::Deribit);
        assert_eq!(event.instrument, "BTC-PERPETUAL");

        let best_bid = event.kind.best_bid.as_ref().unwrap();
        assert_eq!(best_bid.price, dec!(36289.5));
        assert_eq!(best_bid.amount, dec!(4600));

        let best_ask = event.kind.best_ask.as_ref().unwrap();
        assert_eq!(best_ask.price, dec!(36290));
        assert_eq!(best_ask.amount, dec!(53040));
    }

    #[test]
    fn test_from_deribit_ticker_with_zero_prices() {
        use barter_instrument::exchange::ExchangeId;

        let time = Utc::now();
        let ticker = DeribitTicker {
            subscription_id: SubscriptionId::from("ticker|BTC-PERPETUAL"),
            time,
            best_bid_price: Decimal::ZERO,
            best_bid_amount: Decimal::ZERO,
            best_ask_price: Decimal::ZERO,
            best_ask_amount: Decimal::ZERO,
        };

        let market_iter: MarketIter<String, OrderBookL1> =
            (ExchangeId::Deribit, "BTC-PERPETUAL".to_string(), ticker).into();

        let event = market_iter.0[0].as_ref().unwrap();
        assert!(event.kind.best_bid.is_none());
        assert!(event.kind.best_ask.is_none());
    }
}
