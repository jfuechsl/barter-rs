//! Deribit Raw Metrics Aggregation Example
//!
//! This example demonstrates how to subscribe to multiple Deribit market data streams
//! using **authenticated raw feeds** (no aggregation) for BTC-PERPETUAL and aggregate
//! metrics at 1-second intervals.
//!
//! # Authentication
//!
//! This example requires Deribit API credentials. Set them via environment variables:
//!
//! ```bash
//! export DERIBIT_CLIENT_ID="your_client_id"
//! export DERIBIT_CLIENT_SECRET="your_client_secret"
//! ```
//!
//! Or create a `.env` file in the project root (automatically loaded):
//! ```
//! DERIBIT_CLIENT_ID=your_client_id
//! DERIBIT_CLIENT_SECRET=your_client_secret
//! ```
//!
//! # Metrics Collected
//! - Sum of trade volume (from trades feed)
//! - Average mid price (from L1 feed)
//! - Average bid/ask spread (from L1 feed)
//! - Average micro price / volume-weighted mid price (from L2 feed)
//! - **L2 orderbook update count** (sum of updates in aggregation period)
//!
//! After 30 seconds, outputs aggregated metrics in CSV format to stdout.
//!
//! # Usage
//! ```bash
//! DERIBIT_CLIENT_ID="your_id" DERIBIT_CLIENT_SECRET="your_secret" cargo run --example deribit_raw_metrics_aggregation
//! ```

use barter_data::{
    event::DataKind,
    exchange::deribit::{Deribit, DeribitCredentials},
    streams::{Streams, consumer::MarketStreamResult, reconnect::stream::ReconnectingStream},
    subscription::{
        book::{OrderBookEvent, OrderBooksL1, OrderBooksL2},
        trade::PublicTrades,
    },
};
use barter_instrument::instrument::market_data::{
    MarketDataInstrument, kind::MarketDataInstrumentKind,
};
use rust_decimal::Decimal;
use std::collections::HashMap;
use tokio_stream::StreamExt;
use tracing::warn;

/// Aggregates metrics for a single 1-second interval
#[derive(Debug, Default)]
struct IntervalMetrics {
    // Trades
    trade_volume_sum: Decimal,
    trade_count: u64,
    /// Count of trade updates in this interval
    trade_update_count: u64,

    // L1 OrderBook
    mid_price_sum: Decimal,
    mid_price_count: u64,
    spread_sum: Decimal,
    spread_count: u64,
    /// Count of L1 orderbook updates in this interval
    l1_update_count: u64,

    // L2 OrderBook
    micro_price_sum: Decimal,
    micro_price_count: u64,
    /// Count of L2 orderbook updates in this interval (excludes snapshots)
    l2_update_count: u64,
}

impl IntervalMetrics {
    fn average_mid_price(&self) -> Option<f64> {
        if self.mid_price_count > 0 {
            Some(
                (self.mid_price_sum / Decimal::from(self.mid_price_count))
                    .to_string()
                    .parse()
                    .ok()?,
            )
        } else {
            None
        }
    }

    fn average_spread(&self) -> Option<f64> {
        if self.spread_count > 0 {
            Some(
                (self.spread_sum / Decimal::from(self.spread_count))
                    .to_string()
                    .parse()
                    .ok()?,
            )
        } else {
            None
        }
    }

    fn average_micro_price(&self) -> Option<f64> {
        if self.micro_price_count > 0 {
            Some(
                (self.micro_price_sum / Decimal::from(self.micro_price_count))
                    .to_string()
                    .parse()
                    .ok()?,
            )
        } else {
            None
        }
    }

    fn trade_volume(&self) -> f64 {
        self.trade_volume_sum.to_string().parse().unwrap_or(0.0)
    }
}

#[rustfmt::skip]
#[tokio::main]
async fn main() {
    // Load environment variables from .env file if present
    let _ = dotenvy::dotenv();

    // Initialise INFO Tracing log subscriber
    init_logging();

    // Read credentials from environment variables
    let client_id = std::env::var("DERIBIT_CLIENT_ID")
        .expect("DERIBIT_CLIENT_ID environment variable must be set");
    let client_secret = std::env::var("DERIBIT_CLIENT_SECRET")
        .expect("DERIBIT_CLIENT_SECRET environment variable must be set");

    let credentials = DeribitCredentials::new(client_id, client_secret);

    eprintln!("Deribit Raw Metrics Aggregation Example");
    eprintln!("========================================\n");
    eprintln!("Subscribing to BTC-PERPETUAL (raw feeds):");
    eprintln!("  - Public Trades");
    eprintln!("  - OrderBook L1");
    eprintln!("  - OrderBook L2");
    eprintln!("\nCollecting metrics for 30 seconds at 1-second resolution...\n");

    // Build authenticated Deribit connector for raw feeds
    let deribit = Deribit::raw(credentials);

    // Build multi-stream with different subscription types for Deribit BTC-PERPETUAL
    let streams: Streams<MarketStreamResult<MarketDataInstrument, DataKind>> = Streams::builder_multi()
        // PublicTrades Stream (raw)
        .add(Streams::<PublicTrades>::builder()
            .subscribe([
                (deribit.clone(), "btc", "usd", MarketDataInstrumentKind::Perpetual, PublicTrades),
            ])
        )
        // OrderBooksL1 Stream (raw)
        .add(Streams::<OrderBooksL1>::builder()
            .subscribe([
                (deribit.clone(), "btc", "usd", MarketDataInstrumentKind::Perpetual, OrderBooksL1),
            ])
        )
        // OrderBooksL2 Stream (raw)
        .add(Streams::<OrderBooksL2>::builder()
            .subscribe([
                (deribit.clone(), "btc", "usd", MarketDataInstrumentKind::Perpetual, OrderBooksL2),
            ])
        )
        .init()
        .await
        .unwrap();

    eprintln!("Connected to Deribit WebSocket (authenticated raw feeds)\n");

    // Merge all streams
    let mut joined_stream = streams
        .select_all()
        .with_error_handler(|error| warn!(?error, "MarketStream generated error"));

    // Storage for interval metrics
    let mut metrics_by_second: HashMap<u64, IntervalMetrics> = HashMap::new();

    // Start time and deadline
    let start_time = tokio::time::Instant::now();
    let deadline = start_time + tokio::time::Duration::from_secs(30);

    // Process events
    while let Some(event) = joined_stream.next().await {
        // Extract the actual market event from the reconnect Event wrapper
        let market_event = match event {
            barter_data::streams::reconnect::Event::Item(market_event) => market_event,
            barter_data::streams::reconnect::Event::Reconnecting(_) => continue, // Skip reconnection events
        };

        // Calculate which second bucket this event belongs to
        let elapsed = tokio::time::Instant::now().duration_since(start_time);
        let second_bucket = elapsed.as_secs();

        // Get or create the metrics for this second
        let interval = metrics_by_second.entry(second_bucket).or_default();

        // Process event based on kind
        match market_event.kind {
            DataKind::Trade(trade) => {
                interval.trade_volume_sum += Decimal::from_f64_retain(trade.amount).unwrap_or_default();
                interval.trade_count += 1;
                interval.trade_update_count += 1;
            }
            DataKind::OrderBookL1(orderbook_l1) => {
                // Calculate mid price
                if let Some(mid_price) = orderbook_l1.mid_price() {
                    interval.mid_price_sum += mid_price;
                    interval.mid_price_count += 1;
                }

                // Calculate spread
                if let (Some(best_bid), Some(best_ask)) = (orderbook_l1.best_bid, orderbook_l1.best_ask) {
                    let spread = best_ask.price - best_bid.price;
                    interval.spread_sum += spread;
                    interval.spread_count += 1;
                }

                interval.l1_update_count += 1;
            }
            DataKind::OrderBook(orderbook_event) => {
                // Track whether this is an update or snapshot
                let is_update = matches!(&orderbook_event, OrderBookEvent::Update(_));

                // Get the OrderBook from the event (snapshot or update)
                let orderbook = match &orderbook_event {
                    OrderBookEvent::Snapshot(book) => book,
                    OrderBookEvent::Update(book) => book,
                };

                // Calculate volume-weighted mid price (micro price)
                if let Some(micro_price) = orderbook.volume_weighed_mid_price() {
                    interval.micro_price_sum += micro_price;
                    interval.micro_price_count += 1;
                }

                // Count L2 updates (not snapshots)
                if is_update {
                    interval.l2_update_count += 1;
                }
            }
            _ => {
                // Ignore other event types
            }
        }

        // Check if 30 seconds have elapsed
        if tokio::time::Instant::now() >= deadline {
            eprintln!("30 seconds elapsed, processing metrics...\n");
            break;
        }
    }

    // Output CSV to stdout
    output_csv(&metrics_by_second);

    eprintln!("\nExample complete!");
}

/// Output metrics as CSV to stdout
fn output_csv(metrics: &HashMap<u64, IntervalMetrics>) {
    // Print CSV header (includes l2_update_count, l1_update_count, trade_update_count)
    println!(
        "second,trade_volume,avg_mid_price,avg_spread,avg_micro_price,l2_update_count,l1_update_count,trade_update_count"
    );

    // Get sorted seconds
    let mut seconds: Vec<_> = metrics.keys().copied().collect();
    seconds.sort_unstable();

    // Track last known micro_price for forward-filling missing values
    let mut last_micro_price: Option<f64> = None;

    // Print each row
    for second in seconds {
        let interval = &metrics[&second];

        let trade_volume = interval.trade_volume();
        let avg_mid_price = interval
            .average_mid_price()
            .map(|v| format!("{:.2}", v))
            .unwrap_or_else(|| String::from(""));
        let avg_spread = interval
            .average_spread()
            .map(|v| format!("{:.2}", v))
            .unwrap_or_else(|| String::from(""));

        // Use current micro_price if available, otherwise use last known value
        let avg_micro_price = match interval.average_micro_price() {
            Some(price) => {
                last_micro_price = Some(price);
                format!("{:.2}", price)
            }
            None => last_micro_price
                .map(|p| format!("{:.2}", p))
                .unwrap_or_else(|| String::from("")),
        };

        println!(
            "{},{:.4},{},{},{},{},{},{}",
            second,
            trade_volume,
            avg_mid_price,
            avg_spread,
            avg_micro_price,
            interval.l2_update_count,
            interval.l1_update_count,
            interval.trade_update_count
        );
    }
}

// Initialise an INFO `Subscriber` for `Tracing` Json logs and install it as the global default.
fn init_logging() {
    tracing_subscriber::fmt()
        // Filter messages based on the WARN level for less noise
        .with_env_filter(
            tracing_subscriber::filter::EnvFilter::builder()
                .with_default_directive(tracing_subscriber::filter::LevelFilter::WARN.into())
                .from_env_lossy(),
        )
        // Disable colours on release builds
        .with_ansi(cfg!(debug_assertions))
        // Write logs to stderr to keep stdout clean for CSV
        .with_writer(std::io::stderr)
        // Install this Tracing subscriber as global default
        .init()
}
