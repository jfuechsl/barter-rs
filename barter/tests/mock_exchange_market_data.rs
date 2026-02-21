//! Integration test: Market data fan-out to MockExchange
//!
//! This test verifies that when using `init_with_fanout()`, L1 market data events
//! are properly cloned and sent to both the Engine and MockExchange channels.

use barter::execution::market_fanout::MarketFanOut;
use barter_data::books::Level;
use barter_data::{
    event::MarketEvent, streams::consumer::MarketStreamEvent, subscription::book::OrderBookL1,
};
use barter_execution::exchange::mock::MarketPriceUpdate;
use barter_instrument::{
    exchange::ExchangeId,
    instrument::{InstrumentIndex, name::InstrumentNameExchange},
};
use chrono::Utc;
use rust_decimal::Decimal;
use std::time::Duration;

/// Test that MarketFanOut correctly routes L1 updates to registered channels.
#[tokio::test]
async fn market_fan_out_routes_l1_updates() {
    // Create a MockExchange market data channel
    let (market_tx, mut market_rx) = tokio::sync::mpsc::unbounded_channel::<MarketPriceUpdate>();

    // Create MarketFanOut and register the channel
    let mut fan_out = MarketFanOut::new();
    fan_out.register(
        InstrumentIndex(0),
        InstrumentNameExchange::from("BTC-PERPETUAL"),
        market_tx,
    );

    // Create an L1 book update
    let l1 = OrderBookL1::new(
        Utc::now(),
        Some(Level::new(Decimal::from(49000), Decimal::from(1))),
        Some(Level::new(Decimal::from(51000), Decimal::from(1))),
    );

    let event = MarketStreamEvent::Item(MarketEvent {
        time_exchange: Utc::now(),
        time_received: Utc::now(),
        exchange: ExchangeId::Deribit,
        instrument: InstrumentIndex(0),
        kind: l1,
    });

    // Process the event
    fan_out.process_event(&event);

    // Verify the MockExchange received the update
    let received = tokio::time::timeout(Duration::from_millis(100), market_rx.recv()).await;
    assert!(
        received.is_ok(),
        "MockExchange should receive market price update"
    );

    let update = received.unwrap().unwrap();
    assert_eq!(update.best_bid, Decimal::from(49000));
    assert_eq!(update.best_ask, Decimal::from(51000));
}

/// Test that MarketFanOut ignores events with missing bid/ask.
#[tokio::test]
async fn market_fan_out_ignores_incomplete_l1() {
    // Create a MockExchange market data channel
    let (market_tx, mut market_rx) = tokio::sync::mpsc::unbounded_channel::<MarketPriceUpdate>();

    // Create MarketFanOut and register the channel
    let mut fan_out = MarketFanOut::new();
    fan_out.register(
        InstrumentIndex(0),
        InstrumentNameExchange::from("BTC-PERPETUAL"),
        market_tx,
    );

    // Create an incomplete L1 book (missing ask)
    let l1 = OrderBookL1::new(
        Utc::now(),
        Some(Level::new(Decimal::from(49000), Decimal::from(1))),
        None, // Missing ask
    );

    let event = MarketStreamEvent::Item(MarketEvent {
        time_exchange: Utc::now(),
        time_received: Utc::now(),
        exchange: ExchangeId::Deribit,
        instrument: InstrumentIndex(0),
        kind: l1,
    });

    // Process the event
    fan_out.process_event(&event);

    // Verify the MockExchange did NOT receive an update
    let received = tokio::time::timeout(Duration::from_millis(50), market_rx.recv()).await;
    assert!(
        received.is_err(),
        "MockExchange should NOT receive incomplete L1 update"
    );
}
