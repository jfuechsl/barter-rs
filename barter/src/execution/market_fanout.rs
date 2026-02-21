//! Market data fan-out for MockExchange limit order matching.
//!
//! This module provides utilities to "tee" or fan-out the market data stream
//! so that L1 book updates are sent to both the Engine (original behavior)
//! and the MockExchange (for limit order fill triggering).

use barter_data::{
    event::MarketEvent, streams::consumer::MarketStreamEvent, subscription::book::OrderBookL1,
};
use barter_execution::exchange::mock::MarketPriceUpdate;
use barter_instrument::instrument::{InstrumentIndex, name::InstrumentNameExchange};
use futures::Stream;
use std::collections::HashMap;
use tokio::sync::mpsc;

/// A fan-out handle that routes L1 book updates to MockExchange channels.
///
/// Maps each instrument to its corresponding MockExchange market data sender.
#[derive(Debug, Clone)]
pub struct MarketFanOut {
    /// Map from instrument index to the channel sender for that instrument's MockExchange
    channels: HashMap<InstrumentIndex, mpsc::UnboundedSender<MarketPriceUpdate>>,
    /// Map from instrument index to instrument name (for MarketPriceUpdate)
    instrument_names: HashMap<InstrumentIndex, InstrumentNameExchange>,
}

impl MarketFanOut {
    /// Create a new MarketFanOut with no channels registered.
    pub fn new() -> Self {
        Self {
            channels: HashMap::new(),
            instrument_names: HashMap::new(),
        }
    }

    /// Register a MockExchange channel for a specific instrument.
    pub fn register(
        &mut self,
        instrument_index: InstrumentIndex,
        instrument_name: InstrumentNameExchange,
        sender: mpsc::UnboundedSender<MarketPriceUpdate>,
    ) {
        self.channels.insert(instrument_index, sender);
        self.instrument_names
            .insert(instrument_index, instrument_name);
    }

    /// Process a MarketStreamEvent and fan-out L1 updates to registered channels.
    pub fn process_event(&self, event: &MarketStreamEvent<InstrumentIndex, OrderBookL1>) {
        if let MarketStreamEvent::Item(MarketEvent {
            instrument, kind, ..
        }) = event
        {
            // Try to convert L1 to MarketPriceUpdate
            if let Some(update) = MarketPriceUpdate::from_l1(
                self.instrument_names
                    .get(instrument)
                    .cloned()
                    .unwrap_or_else(|| {
                        InstrumentNameExchange::from(format!("unknown-{}", instrument.0))
                    }),
                kind,
            ) {
                // Send to the registered channel for this instrument
                if let Some(sender) = self.channels.get(instrument) {
                    let _ = sender.send(update); // Ignore send errors (channel may be closed)
                }
            }
        }
    }
}

impl Default for MarketFanOut {
    fn default() -> Self {
        Self::new()
    }
}

/// Extension trait for fanning out a market stream.
pub trait MarketStreamFanOut:
    Stream<Item = MarketStreamEvent<InstrumentIndex, OrderBookL1>> + Sized
{
    /// Fan out this stream to both the engine and MockExchange channels.
    ///
    /// Returns a tuple of:
    /// - The original stream (for the engine)
    /// - A future that drives the fan-out (must be spawned)
    fn fan_out(self, fan_out: MarketFanOut) -> (Self, impl std::future::Future<Output = ()>);
}
