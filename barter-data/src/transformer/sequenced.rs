use crate::exchange::Connector;
use crate::{
    Identifier,
    error::DataError,
    event::{MarketEvent, MarketIter},
    subscription::{
        Map,
        book::{OrderBookEvent, OrderBooksL2},
    },
    transformer::ExchangeTransformer,
};
use barter_instrument::exchange::ExchangeId;
use barter_integration::{
    Transformer, protocol::websocket::WsMessage, subscription::SubscriptionId,
};
use std::marker::PhantomData;
use tokio::sync::mpsc;

/// Trait defining the logic for sequencing OrderBook L2 updates.
pub trait OrderBookL2Sequencer: Send + Sized {
    type Update: Identifier<Option<SubscriptionId>>;

    /// Construct a new [`OrderBookL2Sequencer`] from an optional initial [`OrderBookEvent`] snapshot.
    fn new(snapshot: Option<&OrderBookEvent>, sub_id: SubscriptionId) -> Result<Self, DataError>;

    /// Validate the sequence of the update.
    fn validate(&mut self, update: Self::Update) -> Result<Option<Self::Update>, DataError>;
}

#[derive(Debug)]
pub struct SequencedOrderBookL2Transformer<Exchange, InstrumentKey, Sequencer> {
    exchange_id: ExchangeId,
    instrument_map: Map<SequencedInstrument<InstrumentKey, Sequencer>>,
    phantom: PhantomData<Exchange>,
}

#[derive(Debug)]
pub struct SequencedInstrument<InstrumentKey, Sequencer> {
    pub key: InstrumentKey,
    pub sequencer: Sequencer,
}

impl<Exchange, InstrumentKey, Sequencer> ExchangeTransformer<Exchange, InstrumentKey, OrderBooksL2>
    for SequencedOrderBookL2Transformer<Exchange, InstrumentKey, Sequencer>
where
    Exchange: Connector,
    InstrumentKey: Clone + PartialEq + Send + Sync,
    Sequencer: OrderBookL2Sequencer,
    MarketIter<InstrumentKey, OrderBookEvent>: From<(ExchangeId, InstrumentKey, Sequencer::Update)>,
{
    async fn init(
        instrument_map: Map<InstrumentKey>,
        initial_snapshots: &[MarketEvent<InstrumentKey, OrderBookEvent>],
        _: mpsc::UnboundedSender<WsMessage>,
    ) -> Result<Self, DataError> {
        let instrument_map = instrument_map
            .0
            .into_iter()
            .map(|(sub_id, instrument_key)| {
                // Find initial snapshot if it exists
                let snapshot_event = initial_snapshots
                    .iter()
                    .find(|event| event.instrument == instrument_key)
                    .map(|event| &event.kind);

                // If snapshot found, ensure it's a Snapshot event (not Update)
                let snapshot = match snapshot_event {
                    Some(OrderBookEvent::Snapshot(_)) => snapshot_event,
                    Some(OrderBookEvent::Update(_)) => {
                        return Err(DataError::InitialSnapshotInvalid(
                            "expected OrderBookEvent::Snapshot but found OrderBookEvent::Update"
                                .to_string(),
                        ));
                    }
                    None => None,
                };

                let sequencer = Sequencer::new(snapshot, sub_id.clone())?;

                Ok((
                    sub_id,
                    SequencedInstrument {
                        key: instrument_key,
                        sequencer,
                    },
                ))
            })
            .collect::<Result<Map<_>, _>>()?;

        Ok(Self {
            exchange_id: Exchange::ID,
            instrument_map,
            phantom: PhantomData,
        })
    }
}

impl<Exchange, InstrumentKey, Sequencer> Transformer
    for SequencedOrderBookL2Transformer<Exchange, InstrumentKey, Sequencer>
where
    Exchange: Connector,
    InstrumentKey: Clone,
    Sequencer: OrderBookL2Sequencer,
    MarketIter<InstrumentKey, OrderBookEvent>: From<(ExchangeId, InstrumentKey, Sequencer::Update)>,
{
    type Error = DataError;
    type Input = Sequencer::Update;
    type Output = MarketEvent<InstrumentKey, OrderBookEvent>;
    type OutputIter = Vec<Result<Self::Output, Self::Error>>;

    fn transform(&mut self, input: Self::Input) -> Self::OutputIter {
        // Determine if the message has an identifiable SubscriptionId
        let subscription_id = match input.id() {
            Some(subscription_id) => subscription_id,
            None => return vec![],
        };

        // Find Instrument associated with Input and transform
        let instrument = match self.instrument_map.find_mut(&subscription_id) {
            Ok(instrument) => instrument,
            Err(unidentifiable) => return vec![Err(DataError::from(unidentifiable))],
        };

        // Validate sequence
        let valid_update = match instrument.sequencer.validate(input) {
            Ok(Some(valid_update)) => valid_update,
            Ok(None) => return vec![],
            Err(error) => return vec![Err(error)],
        };

        MarketIter::<InstrumentKey, OrderBookEvent>::from((
            self.exchange_id,
            instrument.key.clone(),
            valid_update,
        ))
        .0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        books::{Level, OrderBook},
        subscription::book::OrderBookEvent,
    };
    use barter_integration::subscription::SubscriptionId;

    // Mock sequencer for testing trait contract
    #[derive(Debug)]
    struct MockSequencer {
        last_id: Option<u64>,
        sub_id: SubscriptionId,
    }

    #[derive(Debug, Clone, PartialEq)]
    struct MockUpdate {
        id: u64,
        subscription_id: SubscriptionId,
    }

    impl Identifier<Option<SubscriptionId>> for MockUpdate {
        fn id(&self) -> Option<SubscriptionId> {
            Some(self.subscription_id.clone())
        }
    }

    impl OrderBookL2Sequencer for MockSequencer {
        type Update = MockUpdate;

        fn new(snapshot: Option<&OrderBookEvent>, sub_id: SubscriptionId) -> Result<Self, DataError> {
            // Reject if snapshot is an Update instead of Snapshot
            if let Some(OrderBookEvent::Update(_)) = snapshot {
                return Err(DataError::InitialSnapshotInvalid(
                    "expected Snapshot, got Update".to_string(),
                ));
            }

            let last_id = snapshot.and_then(|event| match event {
                OrderBookEvent::Snapshot(book) => Some(book.sequence()),
                _ => None,
            });

            Ok(Self { last_id, sub_id })
        }

        fn validate(&mut self, update: Self::Update) -> Result<Option<Self::Update>, DataError> {
            match self.last_id {
                None => {
                    // First update - accept and set
                    self.last_id = Some(update.id);
                    Ok(Some(update))
                }
                Some(last_id) => {
                    if update.id == last_id + 1 {
                        self.last_id = Some(update.id);
                        Ok(Some(update))
                    } else if update.id <= last_id {
                        // Stale update
                        Ok(None)
                    } else {
                        // Gap in sequence
                        Err(DataError::InvalidSequence {
                            prev_last_update_id: last_id,
                            first_update_id: update.id,
                        })
                    }
                }
            }
        }
    }

    // Tests for OrderBookL2Sequencer trait
    mod sequencer_trait_tests {
        use super::*;

        #[test]
        fn test_sequencer_new_with_valid_snapshot() {
            let snapshot = OrderBookEvent::Snapshot(OrderBook::new(
                100,
                None,
                Vec::<Level>::new(),
                Vec::<Level>::new(),
            ));
            let sub_id = SubscriptionId::from("test");

            let sequencer = MockSequencer::new(Some(&snapshot), sub_id.clone());
            assert!(sequencer.is_ok());

            let sequencer = sequencer.unwrap();
            assert_eq!(sequencer.last_id, Some(100));
            assert_eq!(sequencer.sub_id, sub_id);
        }

        #[test]
        fn test_sequencer_new_with_update_instead_of_snapshot_errors() {
            let update = OrderBookEvent::Update(OrderBook::new(
                100,
                None,
                Vec::<Level>::new(),
                Vec::<Level>::new(),
            ));
            let sub_id = SubscriptionId::from("test");

            let result = MockSequencer::new(Some(&update), sub_id);
            assert!(result.is_err());

            match result {
                Err(DataError::InitialSnapshotInvalid(_)) => {}
                _ => panic!("Expected InitialSnapshotInvalid error"),
            }
        }

        #[test]
        fn test_sequencer_new_without_snapshot() {
            let sub_id = SubscriptionId::from("test");
            let sequencer = MockSequencer::new(None, sub_id.clone());

            assert!(sequencer.is_ok());
            let sequencer = sequencer.unwrap();
            assert_eq!(sequencer.last_id, None);
        }

        #[test]
        fn test_sequencer_validate_returns_valid_update() {
            let mut sequencer =
                MockSequencer::new(None, SubscriptionId::from("test")).unwrap();

            let update = MockUpdate {
                id: 1,
                subscription_id: SubscriptionId::from("test"),
            };

            let result = sequencer.validate(update.clone());
            assert!(result.is_ok());
            assert_eq!(result.unwrap(), Some(update));
            assert_eq!(sequencer.last_id, Some(1));
        }

        #[test]
        fn test_sequencer_validate_returns_none_for_stale() {
            let snapshot = OrderBookEvent::Snapshot(OrderBook::new(
                100,
                None,
                Vec::<Level>::new(),
                Vec::<Level>::new(),
            ));
            let mut sequencer =
                MockSequencer::new(Some(&snapshot), SubscriptionId::from("test")).unwrap();

            // Try to validate stale update (id <= last_id)
            let stale_update = MockUpdate {
                id: 100,
                subscription_id: SubscriptionId::from("test"),
            };

            let result = sequencer.validate(stale_update);
            assert!(result.is_ok());
            assert_eq!(result.unwrap(), None);
        }

        #[test]
        fn test_sequencer_validate_returns_error_for_gap() {
            let snapshot = OrderBookEvent::Snapshot(OrderBook::new(
                100,
                None,
                Vec::<Level>::new(),
                Vec::<Level>::new(),
            ));
            let mut sequencer =
                MockSequencer::new(Some(&snapshot), SubscriptionId::from("test")).unwrap();

            // Try to validate update with gap
            let update_with_gap = MockUpdate {
                id: 105,
                subscription_id: SubscriptionId::from("test"),
            };

            let result = sequencer.validate(update_with_gap);
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
        fn test_sequencer_validate_sequential_updates() {
            let mut sequencer =
                MockSequencer::new(None, SubscriptionId::from("test")).unwrap();

            for id in 1..=5 {
                let update = MockUpdate {
                    id,
                    subscription_id: SubscriptionId::from("test"),
                };

                let result = sequencer.validate(update.clone());
                assert!(result.is_ok());
                assert_eq!(result.unwrap(), Some(update));
                assert_eq!(sequencer.last_id, Some(id));
            }
        }
    }

    // Note: SequencedOrderBookL2Transformer integration testing is done through
    // concrete implementations like BinanceSpotOrderBookL2Sequencer and
    // BybitOrderBookL2Sequencer. The trait contract is tested above via MockSequencer.
}
