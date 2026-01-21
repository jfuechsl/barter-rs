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

/// Trait defining the logic for sequencing and validating OrderBook L2 updates.
///
/// Many cryptocurrency exchanges stream order book updates as incremental deltas. To maintain
/// an accurate local order book, these updates must be processed in the correct sequence.
/// Different exchanges have different sequencing rules (e.g., Binance Spot vs Binance Futures
/// have subtly different validation logic).
///
/// # Why Sequencing is Needed
///
/// Order book delta streams may:
/// - Arrive out of order due to network latency
/// - Contain stale updates that should be dropped
/// - Have gaps indicating missed messages (requiring re-initialization)
///
/// # Contract
///
/// Implementors must:
/// - Store sequence metadata (e.g., `last_update_id`) to validate incoming updates
/// - Return `Ok(Some(update))` for valid updates that should be applied
/// - Return `Ok(None)` for stale/duplicate updates that should be silently dropped
/// - Return `Err(DataError::InvalidSequence { .. })` when a sequence gap is detected
///
/// # Example Implementations
///
/// See the following exchange-specific implementations:
/// - [`BinanceSpotOrderBookL2Sequencer`](crate::exchange::binance::spot::l2::BinanceSpotOrderBookL2Sequencer)
/// - [`BinanceFuturesUsdOrderBookL2Sequencer`](crate::exchange::binance::futures::l2::BinanceFuturesUsdOrderBookL2Sequencer)
/// - [`BybitOrderBookL2Sequencer`](crate::exchange::bybit::book::l2::BybitOrderBookL2Sequencer)
pub trait OrderBookL2Sequencer: Send + Sized {
    /// The exchange-specific update type containing order book delta information.
    ///
    /// Must implement [`Identifier<Option<SubscriptionId>>`] to enable routing updates
    /// to the correct instrument's sequencer.
    type Update: Identifier<Option<SubscriptionId>>;

    /// Construct a new [`OrderBookL2Sequencer`] from an optional initial snapshot.
    ///
    /// # Arguments
    ///
    /// * `snapshot` - An optional initial order book snapshot. When `Some`, the sequencer
    ///   extracts sequence metadata from the snapshot. When `None`, the implementation
    ///   decides how to handle (some exchanges start from first WebSocket snapshot).
    ///
    /// * `sub_id` - The subscription identifier, used for error reporting.
    ///
    /// # Returns
    ///
    /// * `Ok(Self)` - A properly initialized sequencer
    /// * `Err(DataError::InitialSnapshotMissing(sub_id))` - When snapshot is required but not provided
    /// * `Err(DataError::InitialSnapshotInvalid(_))` - When snapshot is wrong variant
    ///
    /// # Contract for `snapshot: None`
    ///
    /// When `snapshot` is `None`, the implementation should either:
    /// 1. Return an error if an initial snapshot is required (e.g., Binance)
    /// 2. Initialize in a "waiting for snapshot" state (e.g., Bybit)
    fn new(snapshot: Option<&OrderBookEvent>, sub_id: SubscriptionId) -> Result<Self, DataError>;

    /// Validate the sequence of an incoming update.
    ///
    /// # Returns
    ///
    /// * `Ok(Some(update))` - Valid and in-sequence; apply to order book
    /// * `Ok(None)` - Stale or duplicate; silently drop
    /// * `Err(DataError::InvalidSequence { .. })` - Sequence gap detected; re-initialize
    ///
    /// # What `Ok(None)` Means
    ///
    /// Returning `Ok(None)` indicates the update should not be applied. Common reasons:
    /// - Update's sequence number <= last processed (stale)
    /// - Update arrived before initial snapshot (for Bybit-style exchanges)
    fn validate(&mut self, update: Self::Update) -> Result<Option<Self::Update>, DataError>;
}

/// A generic [`ExchangeTransformer`] for OrderBook L2 streams requiring sequence validation.
///
/// This transformer wraps an [`OrderBookL2Sequencer`] implementation to validate the sequence
/// of incoming updates before transforming them into normalized [`MarketEvent`]s.
///
/// # Overview
///
/// 1. Routes incoming updates to the correct instrument's sequencer using subscription ID
/// 2. Validates sequence using [`OrderBookL2Sequencer::validate`]
/// 3. Transforms valid updates into normalized [`MarketEvent<InstrumentKey, OrderBookEvent>`]
/// 4. Drops stale/duplicate updates (when `validate` returns `Ok(None)`)
///
/// # Type Parameters
///
/// * `Exchange` - The exchange type implementing [`Connector`]
/// * `InstrumentKey` - The instrument identifier type in output events
/// * `Sequencer` - The [`OrderBookL2Sequencer`] implementation
///
/// # Usage
///
/// Typically used via a type alias:
///
/// ```ignore
/// pub type BinanceSpotOrderBooksL2Transformer<InstrumentKey> =
///     SequencedOrderBookL2Transformer<BinanceSpot, InstrumentKey, BinanceSpotOrderBookL2Sequencer>;
/// ```
#[derive(Debug)]
pub struct SequencedOrderBookL2Transformer<Exchange, InstrumentKey, Sequencer> {
    exchange_id: ExchangeId,
    instrument_map: Map<SequencedInstrument<InstrumentKey, Sequencer>>,
    phantom: PhantomData<Exchange>,
}

/// Associates an instrument key with its [`OrderBookL2Sequencer`] instance.
///
/// Each instrument in an [`OrderBooksL2`] stream has its own independent sequencer,
/// since sequence numbers are per-instrument.
///
/// # Fields
///
/// * `key` - The instrument identifier cloned into each output [`MarketEvent`]
/// * `sequencer` - The stateful sequencer tracking sequence numbers
#[derive(Debug)]
pub struct SequencedInstrument<InstrumentKey, Sequencer> {
    /// The instrument identifier for output events.
    pub key: InstrumentKey,
    /// The sequencer validating update sequences.
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

        fn new(
            snapshot: Option<&OrderBookEvent>,
            sub_id: SubscriptionId,
        ) -> Result<Self, DataError> {
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
            let mut sequencer = MockSequencer::new(None, SubscriptionId::from("test")).unwrap();

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
            let mut sequencer = MockSequencer::new(None, SubscriptionId::from("test")).unwrap();

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
