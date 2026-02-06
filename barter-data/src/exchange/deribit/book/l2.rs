//! Deribit Level 2 OrderBook WebSocket message.
//!
//! This module provides types for parsing Deribit's book channel, which sends L2 orderbook
//! data with incremental updates. The first message is a full snapshot, subsequent messages
//! are deltas with `new`, `change`, and `delete` actions.

use crate::{
    books::{Level, OrderBook},
    event::{MarketEvent, MarketIter},
    subscription::book::OrderBookEvent,
};
use barter_instrument::exchange::ExchangeId;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

/// Terse type alias for a [`Deribit`](super::super::Deribit) L2 book WebSocket message.
pub type DeribitBookUpdate = super::super::message::DeribitMessage<DeribitBookUpdateData>;

/// [`Deribit`](super::super::Deribit) real-time OrderBook Level2 (book) data.
///
/// Deribit's book channel provides L2 orderbook data with incremental updates.
/// The first message is a full snapshot, subsequent messages are deltas.
///
/// ### Raw Payload Examples
/// See docs: <https://docs.deribit.com/subscriptions/orderbook>
///
/// #### Initial Snapshot
/// ```json
/// {
///   "jsonrpc": "2.0",
///   "method": "subscription",
///   "params": {
///     "channel": "book.BTC-PERPETUAL.100ms",
///     "data": {
///       "timestamp": 1535098298227,
///       "instrument_name": "BTC-PERPETUAL",
///       "change_id": 123456,
///       "bids": [
///         ["new", 36289.5, 4600],
///         ["new", 36288.0, 5000]
///       ],
///       "asks": [
///         ["new", 36290.0, 53040],
///         ["new", 36291.0, 10000]
///       ]
///     }
///   }
/// }
/// ```
///
/// #### Incremental Update
/// ```json
/// {
///   "jsonrpc": "2.0",
///   "method": "subscription",
///   "params": {
///     "channel": "book.BTC-PERPETUAL.100ms",
///     "data": {
///       "timestamp": 1535098298230,
///       "instrument_name": "BTC-PERPETUAL",
///       "change_id": 123457,
///       "prev_change_id": 123456,
///       "bids": [
///         ["change", 36289.5, 4700],
///         ["delete", 36288.0, 0]
///       ],
///       "asks": [
///         ["new", 36289.8, 1000]
///       ]
///     }
///   }
/// }
/// ```
#[derive(Clone, PartialEq, Debug, Deserialize, Serialize)]
pub struct DeribitBookUpdateData {
    #[serde(
        alias = "timestamp",
        deserialize_with = "barter_integration::de::de_u64_epoch_ms_as_datetime_utc"
    )]
    pub time: DateTime<Utc>,
    #[serde(rename = "change_id")]
    pub change_id: u64,
    #[serde(rename = "prev_change_id", default)]
    pub prev_change_id: Option<u64>,
    #[serde(default, deserialize_with = "de_book_levels")]
    pub bids: Vec<DeribitBookLevel>,
    #[serde(default, deserialize_with = "de_book_levels")]
    pub asks: Vec<DeribitBookLevel>,
}

/// Individual level update in a Deribit book message.
#[derive(Clone, PartialEq, Debug, Deserialize, Serialize)]
pub struct DeribitBookLevel {
    pub action: DeribitBookAction,
    pub price: Decimal,
    pub amount: Decimal,
}

/// Book update action type.
#[derive(Copy, Clone, Eq, PartialEq, Debug, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum DeribitBookAction {
    New,
    Change,
    Delete,
}

impl<InstrumentKey> From<(ExchangeId, InstrumentKey, DeribitBookUpdate)>
    for MarketIter<InstrumentKey, OrderBookEvent>
{
    fn from(
        (exchange, instrument, book_update): (ExchangeId, InstrumentKey, DeribitBookUpdate),
    ) -> Self {
        let data = book_update.data;
        // Determine if this is a snapshot before consuming fields
        let is_snapshot = data.prev_change_id.is_none();

        // Convert DeribitBookLevel to Level (handle delete by setting amount to 0)
        let bid_levels: Vec<Level> = data
            .bids
            .into_iter()
            .map(|level| {
                let amount = match level.action {
                    DeribitBookAction::Delete => Decimal::ZERO,
                    _ => level.amount,
                };
                Level::new(level.price, amount)
            })
            .collect();

        let ask_levels: Vec<Level> = data
            .asks
            .into_iter()
            .map(|level| {
                let amount = match level.action {
                    DeribitBookAction::Delete => Decimal::ZERO,
                    _ => level.amount,
                };
                Level::new(level.price, amount)
            })
            .collect();

        let orderbook = OrderBook::new(data.change_id, Some(data.time), bid_levels, ask_levels);

        let kind = if is_snapshot {
            OrderBookEvent::Snapshot(orderbook)
        } else {
            OrderBookEvent::Update(orderbook)
        };

        Self(vec![Ok(MarketEvent {
            time_exchange: data.time,
            time_received: Utc::now(),
            exchange,
            instrument,
            kind,
        })])
    }
}

/// Deserialize Deribit book levels from array format `[action, price, amount]`.
fn de_book_levels<'de, D>(deserializer: D) -> Result<Vec<DeribitBookLevel>, D::Error>
where
    D: serde::de::Deserializer<'de>,
{
    struct BookLevelVisitor;

    impl<'de> serde::de::Visitor<'de> for BookLevelVisitor {
        type Value = Vec<DeribitBookLevel>;

        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("an array of [action, price, amount] tuples")
        }

        fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
        where
            A: serde::de::SeqAccess<'de>,
        {
            let mut levels = Vec::new();

            while let Some(tuple) =
                seq.next_element::<(String, serde_json::Value, serde_json::Value)>()?
            {
                let action = match tuple.0.as_str() {
                    "new" => DeribitBookAction::New,
                    "change" => DeribitBookAction::Change,
                    "delete" => DeribitBookAction::Delete,
                    _ => {
                        return Err(serde::de::Error::custom(format!(
                            "unknown action: {}",
                            tuple.0
                        )));
                    }
                };

                // Handle both string and numeric prices
                let price = parse_decimal(&tuple.1)
                    .map_err(|e| serde::de::Error::custom(format!("invalid price: {}", e)))?;

                let amount = parse_decimal(&tuple.2)
                    .map_err(|e| serde::de::Error::custom(format!("invalid amount: {}", e)))?;

                levels.push(DeribitBookLevel {
                    action,
                    price,
                    amount,
                });
            }

            Ok(levels)
        }
    }

    deserializer.deserialize_seq(BookLevelVisitor)
}

/// Parse a JSON value as Decimal (handles both strings and numbers).
fn parse_decimal(value: &serde_json::Value) -> Result<Decimal, Box<dyn std::error::Error>> {
    match value {
        serde_json::Value::String(s) => Ok(s.parse()?),
        serde_json::Value::Number(n) => Ok(Decimal::from_str_exact(&n.to_string())?),
        _ => Err("expected string or number".into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Identifier;
    use crate::exchange::deribit::message::DeribitMessage;
    use barter_integration::subscription::SubscriptionId;
    use rust_decimal_macros::dec;

    mod de {
        use super::*;
        use barter_integration::subscription::SubscriptionId;

        #[test]
        fn test_deribit_book_snapshot() {
            let input = r#"
            {
                "jsonrpc": "2.0",
                "method": "subscription",
                "params": {
                    "channel": "book.BTC-PERPETUAL.100ms",
                    "data": {
                        "timestamp": 1535098298227,
                        "instrument_name": "BTC-PERPETUAL",
                        "change_id": 123456,
                        "bids": [
                            ["new", 36289.5, 4600],
                            ["new", 36288.0, 5000],
                            ["new", 36287.0, 3000]
                        ],
                        "asks": [
                            ["new", 36290.0, 53040],
                            ["new", 36291.0, 10000]
                        ]
                    }
                }
            }
            "#;

            let actual = serde_json::from_str::<DeribitBookUpdate>(input).unwrap();

            assert_eq!(
                actual.subscription_id,
                SubscriptionId::from("book|BTC-PERPETUAL")
            );
            assert_eq!(actual.data.change_id, 123456);
            assert_eq!(actual.data.prev_change_id, None);
            assert_eq!(actual.data.bids.len(), 3);
            assert_eq!(actual.data.asks.len(), 2);

            assert!(matches!(actual.data.bids[0].action, DeribitBookAction::New));
            assert_eq!(actual.data.bids[0].price, dec!(36289.5));
            assert_eq!(actual.data.bids[0].amount, dec!(4600));

            assert!(matches!(actual.data.asks[0].action, DeribitBookAction::New));
            assert_eq!(actual.data.asks[0].price, dec!(36290.0));
            assert_eq!(actual.data.asks[0].amount, dec!(53040));
        }

        #[test]
        fn test_deribit_book_update() {
            let input = r#"
            {
                "jsonrpc": "2.0",
                "method": "subscription",
                "params": {
                    "channel": "book.BTC-PERPETUAL.100ms",
                    "data": {
                        "timestamp": 1535098298230,
                        "instrument_name": "BTC-PERPETUAL",
                        "change_id": 123457,
                        "prev_change_id": 123456,
                        "bids": [
                            ["change", 36289.5, 4700],
                            ["delete", 36287.0, 0]
                        ],
                        "asks": [
                            ["new", 36289.8, 1000]
                        ]
                    }
                }
            }
            "#;

            let actual = serde_json::from_str::<DeribitBookUpdate>(input).unwrap();

            assert_eq!(actual.data.change_id, 123457);
            assert_eq!(actual.data.prev_change_id, Some(123456));
            assert_eq!(actual.data.bids.len(), 2);
            assert_eq!(actual.data.asks.len(), 1);

            assert!(matches!(
                actual.data.bids[0].action,
                DeribitBookAction::Change
            ));
            assert_eq!(actual.data.bids[0].price, dec!(36289.5));

            assert!(matches!(
                actual.data.bids[1].action,
                DeribitBookAction::Delete
            ));
            assert_eq!(actual.data.bids[1].price, dec!(36287.0));

            assert!(matches!(actual.data.asks[0].action, DeribitBookAction::New));
            assert_eq!(actual.data.asks[0].price, dec!(36289.8));
        }

        #[test]
        fn test_deribit_book_with_string_values() {
            let input = r#"
            {
                "jsonrpc": "2.0",
                "method": "subscription",
                "params": {
                    "channel": "book.ETH-PERPETUAL.100ms",
                    "data": {
                        "timestamp": 1535098298227,
                        "instrument_name": "ETH-PERPETUAL",
                        "change_id": 100,
                        "bids": [
                            ["new", "2000.5", "100.5"],
                            ["new", "2000.0", "200"]
                        ],
                        "asks": [
                            ["new", "2001.0", "50.25"]
                        ]
                    }
                }
            }
            "#;

            let actual = serde_json::from_str::<DeribitBookUpdate>(input).unwrap();

            assert_eq!(actual.data.bids[0].price, dec!(2000.5));
            assert_eq!(actual.data.bids[0].amount, dec!(100.5));
        }

        #[test]
        fn test_deribit_book_empty() {
            let input = r#"
            {
                "jsonrpc": "2.0",
                "method": "subscription",
                "params": {
                    "channel": "book.BTC-PERPETUAL.100ms",
                    "data": {
                        "timestamp": 1535098298227,
                        "instrument_name": "BTC-PERPETUAL",
                        "change_id": 100,
                        "bids": [],
                        "asks": []
                    }
                }
            }
            "#;

            let actual = serde_json::from_str::<DeribitBookUpdate>(input).unwrap();

            assert!(actual.data.bids.is_empty());
            assert!(actual.data.asks.is_empty());
        }
    }

    #[test]
    fn test_from_deribit_book_to_market_iter() {
        let time = Utc::now();
        let book_update = DeribitMessage {
            subscription_id: SubscriptionId::from("book|BTC-PERPETUAL"),
            data: DeribitBookUpdateData {
                time,
                change_id: 123457,
                prev_change_id: Some(123456),
                bids: vec![
                    DeribitBookLevel {
                        action: DeribitBookAction::Change,
                        price: dec!(36289.5),
                        amount: dec!(4700),
                    },
                    DeribitBookLevel {
                        action: DeribitBookAction::Delete,
                        price: dec!(36288.0),
                        amount: Decimal::ZERO,
                    },
                ],
                asks: vec![DeribitBookLevel {
                    action: DeribitBookAction::New,
                    price: dec!(36290.5),
                    amount: dec!(1000),
                }],
            },
        };

        let market_iter: MarketIter<String, OrderBookEvent> = (
            ExchangeId::Deribit,
            "BTC-PERPETUAL".to_string(),
            book_update,
        )
            .into();

        assert_eq!(market_iter.0.len(), 1);
        let event = market_iter.0[0].as_ref().unwrap();
        assert_eq!(event.exchange, ExchangeId::Deribit);
        assert_eq!(event.instrument, "BTC-PERPETUAL");

        // Verify it's an Update (not Snapshot) since prev_change_id was Some
        assert!(
            matches!(&event.kind, OrderBookEvent::Update(_)),
            "Expected Update event since prev_change_id is Some"
        );
    }

    #[test]
    fn test_from_deribit_book_snapshot_to_market_iter() {
        let time = Utc::now();
        let book_update = DeribitMessage {
            subscription_id: SubscriptionId::from("book|BTC-PERPETUAL"),
            data: DeribitBookUpdateData {
                time,
                change_id: 123456,
                prev_change_id: None, // No prev_change_id means snapshot
                bids: vec![DeribitBookLevel {
                    action: DeribitBookAction::New,
                    price: dec!(36289.5),
                    amount: dec!(4600),
                }],
                asks: vec![DeribitBookLevel {
                    action: DeribitBookAction::New,
                    price: dec!(36290.5),
                    amount: dec!(53040),
                }],
            },
        };

        let market_iter: MarketIter<String, OrderBookEvent> = (
            ExchangeId::Deribit,
            "BTC-PERPETUAL".to_string(),
            book_update,
        )
            .into();

        assert_eq!(market_iter.0.len(), 1);
        let event = market_iter.0[0].as_ref().unwrap();

        // Verify it's a Snapshot (not Update) since prev_change_id is None
        assert!(
            matches!(&event.kind, OrderBookEvent::Snapshot(_)),
            "Expected Snapshot event since prev_change_id is None"
        );
    }

    #[test]
    fn test_deribit_book_level_delete_sets_zero_amount() {
        // Verify that delete actions result in levels with zero amount
        let time = Utc::now();
        let book_update = DeribitMessage {
            subscription_id: SubscriptionId::from("book|BTC-PERPETUAL"),
            data: DeribitBookUpdateData {
                time,
                change_id: 123457,
                prev_change_id: Some(123456),
                bids: vec![DeribitBookLevel {
                    action: DeribitBookAction::Delete,
                    price: dec!(36288.0),
                    amount: dec!(5000), // Non-zero amount in message
                }],
                asks: vec![],
            },
        };

        let market_iter: MarketIter<String, OrderBookEvent> = (
            ExchangeId::Deribit,
            "BTC-PERPETUAL".to_string(),
            book_update,
        )
            .into();

        let event = market_iter.0[0].as_ref().unwrap();
        if let OrderBookEvent::Update(orderbook) = &event.kind {
            let bid_levels = orderbook.bids().levels();
            assert_eq!(bid_levels.len(), 1);
            assert_eq!(bid_levels[0].price, dec!(36288.0));
            assert_eq!(bid_levels[0].amount, Decimal::ZERO); // Should be zero despite input
        } else {
            panic!("Expected Update event");
        }
    }

    #[test]
    fn test_deribit_book_action_deserialization() {
        assert!(matches!(
            serde_json::from_str::<DeribitBookAction>("\"new\"").unwrap(),
            DeribitBookAction::New
        ));
        assert!(matches!(
            serde_json::from_str::<DeribitBookAction>("\"change\"").unwrap(),
            DeribitBookAction::Change
        ));
        assert!(matches!(
            serde_json::from_str::<DeribitBookAction>("\"delete\"").unwrap(),
            DeribitBookAction::Delete
        ));
    }

    #[test]
    fn test_is_snapshot() {
        let snapshot = DeribitBookUpdateData {
            time: Utc::now(),
            change_id: 123456,
            prev_change_id: None,
            bids: vec![],
            asks: vec![],
        };
        assert!(snapshot.prev_change_id.is_none());

        let update = DeribitBookUpdateData {
            time: Utc::now(),
            change_id: 123457,
            prev_change_id: Some(123456),
            bids: vec![],
            asks: vec![],
        };
        assert!(update.prev_change_id.is_some());
    }

    #[test]
    fn test_identifier() {
        let update = DeribitMessage {
            subscription_id: SubscriptionId::from("book|BTC-PERPETUAL"),
            data: DeribitBookUpdateData {
                time: Utc::now(),
                change_id: 123456,
                prev_change_id: None,
                bids: vec![],
                asks: vec![],
            },
        };

        let id = update.id();
        assert_eq!(id, Some(SubscriptionId::from("book|BTC-PERPETUAL")));
    }
}
