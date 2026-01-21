use super::BybitOrderBookMessage;
use crate::{
    error::DataError, exchange::bybit::message::BybitPayloadKind,
    subscription::book::OrderBookEvent, transformer::sequenced::OrderBookL2Sequencer,
};
use barter_integration::subscription::SubscriptionId;
use tracing::debug;

#[derive(Debug)]
pub struct BybitOrderBookL2Sequencer {
    last_update_id: Option<u64>,
}

impl OrderBookL2Sequencer for BybitOrderBookL2Sequencer {
    type Update = BybitOrderBookMessage;

    fn new(_: Option<&OrderBookEvent>, _: SubscriptionId) -> Result<Self, DataError> {
        Ok(Self {
            last_update_id: None,
        })
    }

    fn validate(&mut self, update: Self::Update) -> Result<Option<Self::Update>, DataError> {
        if matches!(update.kind, BybitPayloadKind::Snapshot) {
            self.last_update_id = Some(update.data.update_id);
            return Ok(Some(update));
        }

        if let Some(last_update_id) = self.last_update_id {
            if update.data.update_id != last_update_id + 1 {
                return Err(DataError::InvalidSequence {
                    prev_last_update_id: last_update_id,
                    first_update_id: update.data.update_id,
                });
            }
            self.last_update_id = Some(update.data.update_id);
            Ok(Some(update))
        } else {
            debug!("Update message received before initial Snapshot");
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exchange::bybit::book::{BybitLevel, BybitOrderBookInner};
    use barter_integration::subscription::SubscriptionId;
    use chrono::DateTime;
    use rust_decimal_macros::dec;

    fn make_snapshot(update_id: u64, seq: u64) -> BybitOrderBookMessage {
        BybitOrderBookMessage {
            subscription_id: SubscriptionId::from("orderbook.50|BTCUSDT"),
            kind: BybitPayloadKind::Snapshot,
            time: DateTime::from_timestamp_millis(1672304486868).unwrap(),
            data: BybitOrderBookInner {
                bids: vec![BybitLevel {
                    price: dec!(16493.50),
                    amount: dec!(0.006),
                }],
                asks: vec![BybitLevel {
                    price: dec!(16494.00),
                    amount: dec!(0.010),
                }],
                update_id,
                sequence: seq,
            },
        }
    }

    fn make_delta(update_id: u64, seq: u64) -> BybitOrderBookMessage {
        BybitOrderBookMessage {
            subscription_id: SubscriptionId::from("orderbook.50|BTCUSDT"),
            kind: BybitPayloadKind::Delta,
            time: DateTime::from_timestamp_millis(1672304486868).unwrap(),
            data: BybitOrderBookInner {
                bids: vec![BybitLevel {
                    price: dec!(16493.50),
                    amount: dec!(0.007),
                }],
                asks: vec![],
                update_id,
                sequence: seq,
            },
        }
    }

    #[test]
    fn test_bybit_sequencer_new_without_snapshot() {
        // Bybit doesn't require initial HTTP snapshot
        let seq = BybitOrderBookL2Sequencer::new(None, SubscriptionId::from("test"));
        assert!(seq.is_ok());
        assert_eq!(seq.unwrap().last_update_id, None);
    }

    #[test]
    fn test_bybit_sequencer_new_with_snapshot() {
        // Can also initialize with a snapshot
        let snapshot = OrderBookEvent::Snapshot(crate::books::OrderBook::new(
            100,
            None,
            Vec::<crate::books::Level>::new(),
            Vec::<crate::books::Level>::new(),
        ));
        let seq = BybitOrderBookL2Sequencer::new(Some(&snapshot), SubscriptionId::from("test"));
        assert!(seq.is_ok());
    }

    #[test]
    fn test_bybit_sequencer_validate_snapshot() {
        let mut seq = BybitOrderBookL2Sequencer::new(None, SubscriptionId::from("test")).unwrap();

        let snapshot = make_snapshot(100, 1);
        let result = seq.validate(snapshot.clone());

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Some(snapshot));
        assert_eq!(seq.last_update_id, Some(100));
    }

    #[test]
    fn test_bybit_sequencer_validate_delta_after_snapshot() {
        let mut seq = BybitOrderBookL2Sequencer::new(None, SubscriptionId::from("test")).unwrap();

        // First, process snapshot
        let snapshot = make_snapshot(100, 1);
        seq.validate(snapshot).unwrap();

        // Then process delta with correct sequence
        let delta = make_delta(101, 2);
        let result = seq.validate(delta.clone());

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Some(delta));
        assert_eq!(seq.last_update_id, Some(101));
    }

    #[test]
    fn test_bybit_sequencer_validate_delta_before_snapshot_returns_none() {
        let mut seq = BybitOrderBookL2Sequencer::new(None, SubscriptionId::from("test")).unwrap();

        // Process delta before snapshot - should return None (stale)
        let delta = make_delta(101, 2);
        let result = seq.validate(delta);

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), None);
        assert_eq!(seq.last_update_id, None);
    }

    #[test]
    fn test_bybit_sequencer_validate_sequence_gap_returns_error() {
        let mut seq = BybitOrderBookL2Sequencer::new(None, SubscriptionId::from("test")).unwrap();

        // Process snapshot
        let snapshot = make_snapshot(100, 1);
        seq.validate(snapshot).unwrap();

        // Process delta with gap (should be 101, but is 105)
        let delta = make_delta(105, 2);
        let result = seq.validate(delta);

        assert!(result.is_err());
        match result {
            Err(DataError::InvalidSequence {
                prev_last_update_id,
                first_update_id,
            }) => {
                assert_eq!(prev_last_update_id, 100);
                assert_eq!(first_update_id, 105);
            }
            _ => panic!("Expected InvalidSequence error"),
        }
    }

    #[test]
    fn test_bybit_sequencer_validate_sequential_deltas() {
        let mut seq = BybitOrderBookL2Sequencer::new(None, SubscriptionId::from("test")).unwrap();

        // Process snapshot
        let snapshot = make_snapshot(100, 1);
        seq.validate(snapshot).unwrap();

        // Process multiple sequential deltas
        for update_id in 101..=105 {
            let delta = make_delta(update_id, update_id as u64);
            let result = seq.validate(delta.clone());

            assert!(result.is_ok());
            assert_eq!(result.unwrap(), Some(delta));
            assert_eq!(seq.last_update_id, Some(update_id));
        }
    }

    #[test]
    fn test_bybit_sequencer_validate_snapshot_resets_sequence() {
        let mut seq = BybitOrderBookL2Sequencer::new(None, SubscriptionId::from("test")).unwrap();

        // Process first snapshot
        let snapshot1 = make_snapshot(100, 1);
        seq.validate(snapshot1).unwrap();

        // Process delta
        let delta = make_delta(101, 2);
        seq.validate(delta).unwrap();

        // Process new snapshot - should reset sequence
        let snapshot2 = make_snapshot(200, 10);
        let result = seq.validate(snapshot2.clone());

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Some(snapshot2));
        assert_eq!(seq.last_update_id, Some(200));

        // Next delta should start from 201
        let delta2 = make_delta(201, 11);
        let result = seq.validate(delta2.clone());

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Some(delta2));
    }

    #[test]
    fn test_de_bybit_order_book_message() {
        let input = r#"
            {
                "topic": "orderbook.50.BTCUSDT",
                "type": "snapshot",
                "ts": 1672304486868,
                "data": {
                    "b": [["16493.50", "0.006"]],
                    "a": [["16494.00", "0.010"]],
                    "u": 100,
                    "seq": 1
                }
            }
        "#;

        let result = serde_json::from_str::<BybitOrderBookMessage>(input);
        assert!(result.is_ok());

        let message = result.unwrap();
        assert_eq!(
            message.subscription_id,
            SubscriptionId::from("orderbook.50|BTCUSDT")
        );
        assert_eq!(message.kind, BybitPayloadKind::Snapshot);
        assert_eq!(message.data.update_id, 100);
        assert_eq!(message.data.sequence, 1);
        assert_eq!(message.data.bids.len(), 1);
        assert_eq!(message.data.bids[0].price, dec!(16493.50));
        assert_eq!(message.data.bids[0].amount, dec!(0.006));
    }

    #[test]
    fn test_de_bybit_order_book_delta() {
        let input = r#"
            {
                "topic": "orderbook.50.ETHUSDT",
                "type": "delta",
                "ts": 1672304486868,
                "data": {
                    "b": [["1500.00", "1.5"]],
                    "a": [],
                    "u": 101,
                    "seq": 2
                }
            }
        "#;

        let result = serde_json::from_str::<BybitOrderBookMessage>(input);
        assert!(result.is_ok());

        let message = result.unwrap();
        assert_eq!(
            message.subscription_id,
            SubscriptionId::from("orderbook.50|ETHUSDT")
        );
        assert_eq!(message.kind, BybitPayloadKind::Delta);
        assert_eq!(message.data.update_id, 101);
        assert_eq!(message.data.sequence, 2);
    }
}
