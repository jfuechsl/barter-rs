use crate::{
    Identifier,
    books::Level,
    event::{MarketEvent, MarketIter},
    subscription::book::OrderBookL1,
};
use barter_instrument::exchange::ExchangeId;
use barter_integration::subscription::SubscriptionId;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

/// Terse type alias for a [`Deribit`](super::super::Deribit) L1 ticker WebSocket message.
pub type DeribitTicker = DeribitMessage<DeribitTickerData>;

/// [`Deribit`](super::super::Deribit) market data WebSocket message wrapper for L1.
///
/// Deribit wraps all subscription messages in a JSON-RPC 2.0 format with a `params`
/// field containing the `channel` and `data`.
#[derive(Clone, PartialEq, PartialOrd, Debug, Serialize)]
pub struct DeribitMessage<T> {
    pub subscription_id: SubscriptionId,
    pub data: T,
}

impl<'de, T> Deserialize<'de> for DeribitMessage<T>
where
    T: Deserialize<'de>,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::de::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Params<T> {
            channel: String,
            data: T,
        }

        #[derive(Deserialize)]
        struct Wrapper<T> {
            params: Params<T>,
        }

        let wrapper = Wrapper::deserialize(deserializer)?;
        let subscription_id = parse_deribit_channel(&wrapper.params.channel)
            .map_err(|e| serde::de::Error::custom(e))?;
        Ok(DeribitMessage {
            subscription_id,
            data: wrapper.params.data,
        })
    }
}

/// Parse a Deribit channel string (e.g., "ticker.BTC-PERPETUAL.100ms") into a
/// standard Barter subscription ID format (e.g., "ticker|BTC-PERPETUAL").
fn parse_deribit_channel(channel: &str) -> Result<SubscriptionId, String> {
    let parts: Vec<&str> = channel.split('.').collect();
    if parts.len() < 2 {
        return Err(format!("Invalid Deribit channel format: {}", channel));
    }
    Ok(SubscriptionId::from(format!("{}|{}", parts[0], parts[1])))
}

impl<T> Identifier<Option<SubscriptionId>> for DeribitMessage<T> {
    fn id(&self) -> Option<SubscriptionId> {
        Some(self.subscription_id.clone())
    }
}

/// [`Deribit`](super::super::Deribit) real-time OrderBook Level1 (ticker) data.
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
pub struct DeribitTickerData {
    #[serde(
        alias = "timestamp",
        deserialize_with = "barter_integration::de::de_u64_epoch_ms_as_datetime_utc"
    )]
    pub time: DateTime<Utc>,
    pub best_bid_price: Decimal,
    pub best_bid_amount: Decimal,
    pub best_ask_price: Decimal,
    pub best_ask_amount: Decimal,
}

impl<InstrumentKey> From<(ExchangeId, InstrumentKey, DeribitTicker)>
    for MarketIter<InstrumentKey, OrderBookL1>
{
    fn from((exchange_id, instrument, ticker): (ExchangeId, InstrumentKey, DeribitTicker)) -> Self {
        let data = ticker.data;
        let best_ask = if data.best_ask_price.is_zero() {
            None
        } else {
            Some(Level::new(data.best_ask_price, data.best_ask_amount))
        };

        let best_bid = if data.best_bid_price.is_zero() {
            None
        } else {
            Some(Level::new(data.best_bid_price, data.best_bid_amount))
        };

        Self(vec![Ok(MarketEvent {
            time_exchange: data.time,
            time_received: Utc::now(),
            exchange: exchange_id,
            instrument,
            kind: OrderBookL1 {
                last_update_time: data.time,
                best_bid,
                best_ask,
            },
        })])
    }
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
                        "jsonrpc": "2.0",
                        "method": "subscription",
                        "params": {
                            "channel": "ticker.BTC-PERPETUAL.100ms",
                            "data": {
                                "timestamp": 1623059681955,
                                "instrument_name": "BTC-PERPETUAL",
                                "best_bid_price": 36289.5,
                                "best_bid_amount": 4600,
                                "best_ask_price": 36290,
                                "best_ask_amount": 53040,
                                "last_price": 36289.5,
                                "mark_price": 36288.31
                            }
                        }
                    }
                    "#,
                    expected: DeribitTicker {
                        subscription_id: SubscriptionId::from("ticker|BTC-PERPETUAL"),
                        data: DeribitTickerData {
                            time,
                            best_bid_price: dec!(36289.5),
                            best_bid_amount: dec!(4600),
                            best_ask_price: dec!(36290),
                            best_ask_amount: dec!(53040),
                        },
                    },
                },
                TestCase {
                    // TC1: valid DeribitTicker with string prices (some versions send strings)
                    input: r#"
                    {
                        "jsonrpc": "2.0",
                        "method": "subscription",
                        "params": {
                            "channel": "ticker.ETH-PERPETUAL.100ms",
                            "data": {
                                "timestamp": 1623059681955,
                                "instrument_name": "ETH-PERPETUAL",
                                "best_bid_price": "2000.5",
                                "best_bid_amount": "100.5",
                                "best_ask_price": "2001.0",
                                "best_ask_amount": "200.25",
                                "last_price": "2000.75",
                                "mark_price": "2000.5"
                            }
                        }
                    }
                    "#,
                    expected: DeribitTicker {
                        subscription_id: SubscriptionId::from("ticker|ETH-PERPETUAL"),
                        data: DeribitTickerData {
                            time,
                            best_bid_price: dec!(2000.5),
                            best_bid_amount: dec!(100.5),
                            best_ask_price: dec!(2001.0),
                            best_ask_amount: dec!(200.25),
                        },
                    },
                },
            ];

            for (index, test) in tests.into_iter().enumerate() {
                let actual = serde_json::from_str::<DeribitTicker>(test.input).unwrap();
                let actual = DeribitTicker {
                    subscription_id: actual.subscription_id,
                    data: DeribitTickerData {
                        time,
                        ..actual.data
                    },
                };

                assert_eq!(
                    actual.subscription_id, test.expected.subscription_id,
                    "TC{}: subscription_id mismatch",
                    index
                );
                assert_eq!(
                    actual.data.best_bid_price, test.expected.data.best_bid_price,
                    "TC{}: best_bid_price mismatch",
                    index
                );
                assert_eq!(
                    actual.data.best_bid_amount, test.expected.data.best_bid_amount,
                    "TC{}: best_bid_amount mismatch",
                    index
                );
                assert_eq!(
                    actual.data.best_ask_price, test.expected.data.best_ask_price,
                    "TC{}: best_ask_price mismatch",
                    index
                );
                assert_eq!(
                    actual.data.best_ask_amount, test.expected.data.best_ask_amount,
                    "TC{}: best_ask_amount mismatch",
                    index
                );
            }
        }

        #[test]
        fn test_deribit_ticker_future() {
            let input = r#"
            {
                "jsonrpc": "2.0",
                "method": "subscription",
                "params": {
                    "channel": "ticker.BTC-27SEP24.100ms",
                    "data": {
                        "timestamp": 1623059681955,
                        "instrument_name": "BTC-27SEP24",
                        "best_bid_price": 35000,
                        "best_bid_amount": 1000,
                        "best_ask_price": 35001,
                        "best_ask_amount": 2000,
                        "last_price": 35000.5,
                        "mark_price": 35000
                    }
                }
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
            data: DeribitTickerData {
                time,
                best_bid_price: dec!(36289.5),
                best_bid_amount: dec!(4600),
                best_ask_price: dec!(36290),
                best_ask_amount: dec!(53040),
            },
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
            data: DeribitTickerData {
                time,
                best_bid_price: Decimal::ZERO,
                best_bid_amount: Decimal::ZERO,
                best_ask_price: Decimal::ZERO,
                best_ask_amount: Decimal::ZERO,
            },
        };

        let market_iter: MarketIter<String, OrderBookL1> =
            (ExchangeId::Deribit, "BTC-PERPETUAL".to_string(), ticker).into();

        let event = market_iter.0[0].as_ref().unwrap();
        assert!(event.kind.best_bid.is_none());
        assert!(event.kind.best_ask.is_none());
    }
}
