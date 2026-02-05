//! Deribit OrderBook L1 (Ticker) WebSocket Stream Example
//!
//! This example demonstrates how to connect to Deribit's WebSocket API
//! and stream Level 1 orderbook data (best bid/ask) using the ticker channel.
//!
//! # Usage
//! ```bash
//! cargo run --example deribit_orderbooks_l1
//! ```

use barter_data::{
    exchange::deribit::Deribit,
    streams::{Streams, reconnect::stream::ReconnectingStream},
    subscription::book::OrderBooksL1,
};
use barter_instrument::{
    exchange::ExchangeId, instrument::market_data::kind::MarketDataInstrumentKind,
};
use tokio_stream::StreamExt;
use tracing::{info, warn};

#[rustfmt::skip]
#[tokio::main]
async fn main() {
    // Initialise INFO Tracing log subscriber
    init_logging();

    println!("Deribit OrderBook L1 Stream Example");
    println!("====================================\n");

    // Initialise OrderBooksL1 Streams for Deribit
    // '--> each call to StreamBuilder::subscribe() creates a separate WebSocket connection
    let mut streams = Streams::<OrderBooksL1>::builder()

        // Separate WebSocket connection for BTC-PERPETUAL stream
        .subscribe([
            (Deribit, "btc", "usd", MarketDataInstrumentKind::Perpetual, OrderBooksL1),
        ])

        // Separate WebSocket connection for ETH-PERPETUAL stream
        .subscribe([
            (Deribit, "eth", "usd", MarketDataInstrumentKind::Perpetual, OrderBooksL1),
        ])
        .init()
        .await
        .unwrap();

    println!("Connected to Deribit WebSocket");
    println!("Streaming L1 orderbook for 30 seconds...\n");

    // Select the ExchangeId::Deribit stream
    let mut deribit_stream = streams
        .select(ExchangeId::Deribit)
        .unwrap()
        .with_error_handler(|error| warn!(?error, "MarketStream generated error"));

    // Stream L1 updates for 30 seconds
    let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(30);

    while let Some(event) = deribit_stream.next().await {
        info!("{event:?}");

        // Check if 30 seconds have elapsed
        if tokio::time::Instant::now() >= deadline {
            println!("\n30 second timeout reached");
            break;
        }
    }

    println!("\nExample complete!");
}

// Initialise an INFO `Subscriber` for `Tracing` Json logs and install it as the global default.
fn init_logging() {
    tracing_subscriber::fmt()
        // Filter messages based on the INFO
        .with_env_filter(
            tracing_subscriber::filter::EnvFilter::builder()
                .with_default_directive(tracing_subscriber::filter::LevelFilter::INFO.into())
                .from_env_lossy(),
        )
        // Disable colours on release builds
        .with_ansi(cfg!(debug_assertions))
        // Install this Tracing subscriber as global default
        .init()
}
