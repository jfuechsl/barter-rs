//! Deribit Spot Metrics Aggregation Example
//!
//! This example demonstrates how to subscribe to multiple Deribit market data streams
//! (PublicTrades, OrderBooksL1, OrderBooksL2) for BTC_USDC Spot and aggregate metrics
//! at 1-second intervals.
//!
//! Metrics collected:
//! - Sum of trade volume (from trades feed)
//! - Average mid price (from L1 feed)
//! - Average bid/ask spread (from L1 feed)
//! - Average micro price / volume-weighted mid price (from L2 feed)
//!
//! After 30 seconds, outputs aggregated metrics in CSV format to stdout.
//!
//! # Usage
//! ```bash
//! cargo run --example deribit_spot_metrics_aggregation
//! ```

use barter_data::{
    event::DataKind,
    exchange::deribit::Deribit,
    streams::{Streams, consumer::MarketStreamResult, reconnect::stream::ReconnectingStream},
    subscription::{
        book::{OrderBooksL1, OrderBooksL2},
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

    // L1 OrderBook
    mid_price_sum: Decimal,
    mid_price_count: u64,
    spread_sum: Decimal,
    spread_count: u64,

    // L2 OrderBook
    micro_price_sum: Decimal,
    micro_price_count: u64,
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
    // Initialise INFO Tracing log subscriber
    init_logging();

    eprintln!("Deribit Spot Metrics Aggregation Example");
    eprintln!("=========================================\n");
    eprintln!("Subscribing to BTC_USDC Spot:");
    eprintln!("  - Public Trades");
    eprintln!("  - OrderBook L1");
    eprintln!("  - OrderBook L2");
    eprintln!("\nCollecting metrics for 30 seconds at 1-second resolution...\n");

    // Build multi-stream with different subscription types for Deribit BTC_USDC Spot
    let streams: Streams<MarketStreamResult<MarketDataInstrument, DataKind>> = Streams::builder_multi()
        // PublicTrades Stream
        .add(Streams::<PublicTrades>::builder()
            .subscribe([
                (Deribit::default(), "btc", "usdc", MarketDataInstrumentKind::Spot, PublicTrades),
            ])
        )
        // OrderBooksL1 Stream
        .add(Streams::<OrderBooksL1>::builder()
            .subscribe([
                (Deribit::default(), "btc", "usdc", MarketDataInstrumentKind::Spot, OrderBooksL1),
            ])
        )
        // OrderBooksL2 Stream
        .add(Streams::<OrderBooksL2>::builder()
            .subscribe([
                (Deribit::default(), "btc", "usdc", MarketDataInstrumentKind::Spot, OrderBooksL2),
            ])
        )
        .init()
        .await
        .unwrap();

    eprintln!("Connected to Deribit WebSocket\n");

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
            }
            DataKind::OrderBook(orderbook_event) => {
                // Get the OrderBook from the event (snapshot or update)
                let orderbook = match &orderbook_event {
                    barter_data::subscription::book::OrderBookEvent::Snapshot(book) => book,
                    barter_data::subscription::book::OrderBookEvent::Update(book) => book,
                };

                // Calculate volume-weighted mid price (micro price)
                if let Some(micro_price) = orderbook.volume_weighed_mid_price() {
                    interval.micro_price_sum += micro_price;
                    interval.micro_price_count += 1;
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
    // Print CSV header
    println!("second,trade_volume,avg_mid_price,avg_spread,avg_micro_price");

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
            "{},{:.4},{},{},{}",
            second, trade_volume, avg_mid_price, avg_spread, avg_micro_price
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
