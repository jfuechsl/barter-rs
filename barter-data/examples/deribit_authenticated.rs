//! Deribit Authenticated Raw Feeds Example
//!
//! This example demonstrates how to use `DynamicStreams::init_with_connectors()` to provide
//! authenticated Deribit credentials for accessing raw (unaggregated) market data feeds.
//!
//! Raw feeds provide tick-by-tick data without aggregation, but require authentication.
//! See: <https://support.deribit.com/hc/en-us/articles/29592500256669-Market-Data-Collection-Best-Practices>
//!
//! # Usage
//!
//! Set environment variables for your Deribit API credentials:
//! ```bash
//! export DERIBIT_CLIENT_ID="your_client_id"
//! export DERIBIT_CLIENT_SECRET="your_client_secret"
//! cargo run --example deribit_authenticated
//! ```
//!
//! # Note
//!
//! This example will fail to connect if you don't provide valid credentials.
//! You can obtain API credentials from your Deribit account settings.

use barter_data::{
    event::DataKind,
    exchange::{
        connector_factory::{ConnectorFactory, ExchangeConnector},
        deribit::DeribitCredentials,
    },
    streams::{
        builder::dynamic::DynamicStreams, consumer::MarketStreamResult,
        reconnect::stream::ReconnectingStream,
    },
    subscription::SubKind,
};
use barter_instrument::{
    exchange::ExchangeId,
    instrument::market_data::{MarketDataInstrument, kind::MarketDataInstrumentKind},
};
use futures::StreamExt;
use tracing::{info, warn};

#[rustfmt::skip]
#[tokio::main]
async fn main() {
    // Initialise INFO Tracing log subscriber
    init_logging();

    println!("Deribit Authenticated Raw Feeds Example");
    println!("========================================\n");

    // Load credentials from environment (NEVER hardcode secrets in production!)
    let client_id = std::env::var("DERIBIT_CLIENT_ID")
        .expect("DERIBIT_CLIENT_ID environment variable must be set");
    let client_secret = std::env::var("DERIBIT_CLIENT_SECRET")
        .expect("DERIBIT_CLIENT_SECRET environment variable must be set");

    println!("Creating authenticated Deribit connector...");

    // Create authenticated Deribit connector with raw feeds
    // Raw feeds require authentication and provide unaggregated tick-by-tick data
    let deribit = barter_data::exchange::deribit::Deribit::raw(DeribitCredentials::new(client_id, client_secret));

    // Create connector factory with our authenticated Deribit instance
    let factory = ConnectorFactory::new()
        .with_connector(ExchangeConnector::Deribit(deribit));

    println!("Initializing streams with custom connector factory...");

    use ExchangeId::*;
    use MarketDataInstrumentKind::*;
    use SubKind::*;

    // Initialize DynamicStreams using init_with_connectors with our factory
    // This will use our authenticated Deribit connector instead of the default
    let streams = match DynamicStreams::init_with_connectors(
        vec![
            // Batch 1: BTC raw trades via authenticated connection
            vec![
                (Deribit, "btc", "usd", Perpetual, PublicTrades),
            ],
            // Batch 2: ETH raw trades via separate authenticated connection
            vec![
                (Deribit, "eth", "usd", Perpetual, PublicTrades),
            ],
        ],
        &factory,
    ).await {
        Ok(streams) => {
            println!("Successfully connected to Deribit with authentication!");
            streams
        }
        Err(e) => {
            eprintln!("Failed to connect: {:?}", e);
            eprintln!("\nNote: Make sure your DERIBIT_CLIENT_ID and DERIBIT_CLIENT_SECRET are valid.");
            std::process::exit(1);
        }
    };

    println!("\nStreaming raw trades for 30 seconds...");
    println!("(Raw feeds provide tick-by-tick unaggregated data)\n");

    // Get the trades stream and process events
    let mut trades = streams
        .select_all::<MarketStreamResult<MarketDataInstrument, DataKind>>()
        .with_error_handler(|error| warn!(?error, "MarketStream generated error"));

    // Stream trades for 30 seconds
    let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(30);

    while let Some(event) = trades.next().await {
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
