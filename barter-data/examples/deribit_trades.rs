//! Deribit Public Trades WebSocket Stream Example
//!
//! This example demonstrates how to connect to Deribit's WebSocket API
//! and stream public trade data for perpetual futures.
//!
//! # Usage
//! ```bash
//! cargo run --example deribit_trades
//! ```

use barter_data::{
    exchange::deribit::Deribit,
    streams::{Streams, reconnect::stream::ReconnectingStream},
    subscription::trade::PublicTrades,
};
use barter_instrument::instrument::market_data::kind::MarketDataInstrumentKind;
use futures_util::StreamExt;
use tracing::{info, warn};

#[rustfmt::skip]
#[tokio::main]
async fn main() {
    // Initialise INFO Tracing log subscriber
    init_logging();

    println!("Deribit Public Trades Stream Example");
    println!("=====================================\n");

    // Initialise PublicTrades Streams for Deribit
    // '--> each call to StreamBuilder::subscribe() creates a separate WebSocket connection
    let streams = Streams::<PublicTrades>::builder()

        // Separate WebSocket connection for BTC-PERPETUAL stream
        .subscribe([
            (Deribit::default(), "btc", "usd", MarketDataInstrumentKind::Perpetual, PublicTrades),
        ])

        // Separate WebSocket connection for ETH-PERPETUAL stream
        .subscribe([
            (Deribit::default(), "eth", "usd", MarketDataInstrumentKind::Perpetual, PublicTrades),
        ])
        .init()
        .await
        .unwrap();

    println!("Connected to Deribit WebSocket");
    println!("Streaming trades for 30 seconds...\n");

    // Select and merge every exchange Stream using futures_util::stream::select_all
    let mut joined_stream = streams
        .select_all()
        .with_error_handler(|error| warn!(?error, "MarketStream generated error"));

    // Stream trades for 30 seconds
    let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(30);

    while let Some(event) = joined_stream.next().await {
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
