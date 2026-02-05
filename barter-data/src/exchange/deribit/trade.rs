use crate::{
    Identifier,
    event::{MarketEvent, MarketIter},
    subscription::trade::PublicTrade,
};
use barter_instrument::{Side, exchange::ExchangeId};
use barter_integration::subscription::SubscriptionId;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Terse type alias for a [`Deribit`](super::Deribit) real-time trades WebSocket message.
pub type DeribitTrades = DeribitMessage<DeribitTrade>;

/// [`Deribit`](super::Deribit) market data WebSocket message wrapper.
///
/// Deribit wraps all subscription messages in a JSON-RPC 2.0 format with a `params`
/// field containing the `channel` and `data`.
///
/// ### Raw Payload Examples
/// See docs: <https://docs.deribit.com/subscriptions/trades>
/// #### Perpetual Trade
/// ```json
/// {
///   "jsonrpc": "2.0",
///   "method": "subscription",
///   "params": {
///     "channel": "trades.BTC-PERPETUAL.raw",
///     "data": [
///       {
///         "trade_seq": 2,
///         "trade_id": "48079289",
///         "timestamp": 1590484589306,
///         "tick_direction": 2,
///         "price": 36289.5,
///         "mark_price": 36288.31,
///         "instrument_name": "BTC-PERPETUAL",
///         "index_price": 36297.02,
///         "direction": "sell",
///         "amount": 10.5
///       }
///     ]
///   }
/// }
/// ```
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Serialize)]
pub struct DeribitMessage<T> {
    pub subscription_id: SubscriptionId,
    pub data: Vec<T>,
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
            data: Vec<T>,
        }

        #[derive(Deserialize)]
        struct Wrapper<T> {
            params: Params<T>,
        }

        let wrapper = Wrapper::deserialize(deserializer)?;
        Ok(DeribitMessage {
            subscription_id: SubscriptionId::from(wrapper.params.channel),
            data: wrapper.params.data,
        })
    }
}

impl<T> Identifier<Option<SubscriptionId>> for DeribitMessage<T> {
    fn id(&self) -> Option<SubscriptionId> {
        Some(self.subscription_id.clone())
    }
}

/// [`Deribit`](super::Deribit) real-time trade WebSocket message.
///
/// See [`DeribitMessage`] for full raw payload examples.
///
/// See docs: <https://docs.deribit.com/subscriptions/trades>
#[derive(Clone, PartialEq, PartialOrd, Debug, Deserialize, Serialize)]
pub struct DeribitTrade {
    #[serde(rename = "trade_id")]
    pub id: String,
    #[serde(
        rename = "timestamp",
        deserialize_with = "barter_integration::de::de_u64_epoch_ms_as_datetime_utc"
    )]
    pub time: DateTime<Utc>,
    #[serde(deserialize_with = "barter_integration::de::de_str")]
    pub price: f64,
    #[serde(deserialize_with = "barter_integration::de::de_str")]
    pub amount: f64,
    pub direction: DeribitSide,
}

/// Deribit trade side ("buy" or "sell").
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum DeribitSide {
    Buy,
    Sell,
}

impl From<DeribitSide> for Side {
    fn from(side: DeribitSide) -> Self {
        match side {
            DeribitSide::Buy => Side::Buy,
            DeribitSide::Sell => Side::Sell,
        }
    }
}

impl<InstrumentKey: Clone> From<(ExchangeId, InstrumentKey, DeribitTrades)>
    for MarketIter<InstrumentKey, PublicTrade>
{
    fn from((exchange, instrument, trades): (ExchangeId, InstrumentKey, DeribitTrades)) -> Self {
        trades
            .data
            .into_iter()
            .map(|trade| {
                Ok(MarketEvent {
                    time_exchange: trade.time,
                    time_received: Utc::now(),
                    exchange,
                    instrument: instrument.clone(),
                    kind: PublicTrade {
                        id: trade.id,
                        price: trade.price,
                        amount: trade.amount,
                        side: trade.direction.into(),
                    },
                })
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use barter_integration::de::datetime_utc_from_epoch_duration;
    use std::time::Duration;

    mod de {
        use super::*;
        use barter_integration::error::SocketError;

        #[test]
        fn test_deribit_message_trades() {
            let input = r#"
            {
                "jsonrpc": "2.0",
                "method": "subscription",
                "params": {
                    "channel": "trades.BTC-PERPETUAL.raw",
                    "data": [
                        {
                            "trade_seq": 2,
                            "trade_id": "48079289",
                            "timestamp": 1590484589306,
                            "tick_direction": 2,
                            "price": "36289.5",
                            "mark_price": "36288.31",
                            "instrument_name": "BTC-PERPETUAL",
                            "index_price": "36297.02",
                            "direction": "sell",
                            "amount": "10.5"
                        }
                    ]
                }
            }
            "#;

            let actual = serde_json::from_str::<DeribitTrades>(input);
            let expected_time =
                datetime_utc_from_epoch_duration(Duration::from_millis(1590484589306));

            let expected: Result<DeribitTrades, SocketError> = Ok(DeribitTrades {
                subscription_id: SubscriptionId::from("trades.BTC-PERPETUAL.raw"),
                data: vec![DeribitTrade {
                    id: "48079289".to_string(),
                    time: expected_time,
                    price: 36289.5,
                    amount: 10.5,
                    direction: DeribitSide::Sell,
                }],
            });

            match (actual, expected) {
                (Ok(actual), Ok(expected)) => {
                    assert_eq!(actual.subscription_id, expected.subscription_id);
                    assert_eq!(actual.data.len(), expected.data.len());
                    assert_eq!(actual.data[0].id, expected.data[0].id);
                    assert_eq!(actual.data[0].price, expected.data[0].price);
                    assert_eq!(actual.data[0].amount, expected.data[0].amount);
                }
                (actual, expected) => {
                    panic!(
                        "Test failed because actual != expected.\nActual: {actual:?}\nExpected: {expected:?}\n"
                    );
                }
            }
        }

        #[test]
        fn test_deribit_message_multiple_trades() {
            let input = r#"
            {
                "jsonrpc": "2.0",
                "method": "subscription",
                "params": {
                    "channel": "trades.ETH-PERPETUAL.raw",
                    "data": [
                        {
                            "trade_seq": 1,
                            "trade_id": "111",
                            "timestamp": 1590484589306,
                            "tick_direction": 0,
                            "price": "2000.0",
                            "mark_price": "2001.0",
                            "instrument_name": "ETH-PERPETUAL",
                            "index_price": "2000.5",
                            "direction": "buy",
                            "amount": "1.5"
                        },
                        {
                            "trade_seq": 2,
                            "trade_id": "112",
                            "timestamp": 1590484589310,
                            "tick_direction": 1,
                            "price": "2000.5",
                            "mark_price": "2001.0",
                            "instrument_name": "ETH-PERPETUAL",
                            "index_price": "2000.5",
                            "direction": "sell",
                            "amount": "2.0"
                        }
                    ]
                }
            }
            "#;

            let actual = serde_json::from_str::<DeribitTrades>(input).unwrap();
            assert_eq!(actual.data.len(), 2);
            assert_eq!(actual.data[0].id, "111");
            assert_eq!(actual.data[1].id, "112");
            assert!(matches!(actual.data[0].direction, DeribitSide::Buy));
            assert!(matches!(actual.data[1].direction, DeribitSide::Sell));
        }

        #[test]
        fn test_deribit_trade_option() {
            let input = r#"
            {
                "jsonrpc": "2.0",
                "method": "subscription",
                "params": {
                    "channel": "trades.BTC-27SEP24-50000-C.raw",
                    "data": [
                        {
                            "trade_seq": 1,
                            "trade_id": "OPTION123",
                            "timestamp": 1590484589306,
                            "tick_direction": 0,
                            "price": "0.005",
                            "mark_price": "0.0051",
                            "instrument_name": "BTC-27SEP24-50000-C",
                            "index_price": "45000",
                            "direction": "buy",
                            "amount": "10"
                        }
                    ]
                }
            }
            "#;

            let actual = serde_json::from_str::<DeribitTrades>(input).unwrap();
            assert_eq!(actual.data.len(), 1);
            assert_eq!(actual.data[0].id, "OPTION123");
            assert_eq!(actual.data[0].price, 0.005);
        }
    }

    #[test]
    fn test_deribit_side_into_side() {
        assert!(matches!(Side::from(DeribitSide::Buy), Side::Buy));
        assert!(matches!(Side::from(DeribitSide::Sell), Side::Sell));
    }

    #[test]
    fn test_from_deribit_to_market_iter() {
        use barter_instrument::exchange::ExchangeId;

        let time = Utc::now();
        let trades = DeribitTrades {
            subscription_id: SubscriptionId::from("trades.BTC-PERPETUAL.raw"),
            data: vec![DeribitTrade {
                id: "12345".to_string(),
                time,
                price: 50000.0,
                amount: 1.5,
                direction: DeribitSide::Buy,
            }],
        };

        let market_iter: MarketIter<String, PublicTrade> =
            (ExchangeId::Deribit, "BTC-PERPETUAL".to_string(), trades).into();

        assert_eq!(market_iter.0.len(), 1);
        let event = market_iter.0[0].as_ref().unwrap();
        assert_eq!(event.exchange, ExchangeId::Deribit);
        assert_eq!(event.instrument, "BTC-PERPETUAL");
        assert_eq!(event.kind.price, 50000.0);
        assert_eq!(event.kind.amount, 1.5);
        assert!(matches!(event.kind.side, Side::Buy));
    }
}
