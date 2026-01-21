use crate::{
    Identifier,
    error::DataError,
    exchange::StreamSelector,
    instrument::InstrumentData,
    streams::{
        BoxedMarketStream,
        consumer::{MarketStreamResult, STREAM_RECONNECTION_POLICY, init_market_stream},
        reconnect::stream::ReconnectingStream,
    },
    subscription::{
        SubKind, Subscription, SubscriptionKind,
        book::{OrderBookEvent, OrderBookL1, OrderBooksL1, OrderBooksL2},
        candle::Candle,
        liquidation::{Liquidation, Liquidations},
        trade::{PublicTrade, PublicTrades},
    },
};
use barter_macro::define_stream_connectors;

use barter_instrument::exchange::ExchangeId;
use barter_integration::{
    Validator,
    channel::{UnboundedRx, UnboundedTx, mpsc_unbounded},
    error::SocketError,
};
use fnv::FnvHashMap;
use futures::{Stream, stream::SelectAll};
use futures_util::{StreamExt, future::try_join_all};
use itertools::Itertools;
use std::{
    collections::HashSet,
    fmt::{Debug, Display},
    sync::Arc,
};
use tokio::task::JoinHandle;
use tokio_stream::wrappers::UnboundedReceiverStream;
use vecmap::VecMap;

pub mod indexed;

define_stream_connectors! {
    BinanceSpot => [PublicTrades, OrderBooksL1, OrderBooksL2],
    BinanceFuturesUsd => [PublicTrades, OrderBooksL1, OrderBooksL2, Liquidations],
    Bitfinex => [PublicTrades],
    Bitmex => [PublicTrades],
    BybitSpot => [PublicTrades, OrderBooksL1, OrderBooksL2],
    BybitPerpetualsUsd => [PublicTrades, OrderBooksL1, OrderBooksL2],
    Coinbase => [PublicTrades],
    GateioSpot => [PublicTrades],
    GateioFuturesUsd => [PublicTrades],
    GateioFuturesBtc => [PublicTrades],
    GateioPerpetualsUsd => [PublicTrades],
    GateioPerpetualsBtc => [PublicTrades],
    GateioOptions => [PublicTrades],
    Kraken => [PublicTrades, OrderBooksL1],
    Okx => [PublicTrades],
}

/// Initialize a [`MarketStream`] and spawn a task to forward events to a channel.
///
/// Combines [`init_boxed_stream`] and [`spawn_forward`] into a single operation.
///
/// # Type Parameters
///
/// * `Exchange` - The exchange connector type
/// * `Instrument` - The instrument data type
/// * `Kind` - The subscription kind
///
/// # Arguments
///
/// * `exchange` - The exchange instance
/// * `subscriptions` - Subscriptions to initialize
/// * `sender` - Channel sender for market events
/// * `kind` - The subscription kind marker
///
/// # Returns
///
/// * `Ok(JoinHandle<()>)` - Handle to the forwarding task
/// * `Err(DataError)` - If initialization fails
async fn init_and_forward<Exchange, Instrument, Kind>(
    exchange: Exchange,
    subscriptions: Vec<Subscription<ExchangeId, Instrument, SubKind>>,
    sender: UnboundedTx<MarketStreamResult<Instrument::Key, Kind::Event>>,
    kind: Kind,
) -> Result<JoinHandle<()>, DataError>
where
    Exchange: StreamSelector<Instrument, Kind> + Clone + Send + 'static,
    Instrument: InstrumentData + Ord + Display + Send + Sync + 'static,
    Instrument::Key: Send + Sync + Debug + Clone,
    Kind: SubscriptionKind + Display + Copy + Send + Sync + 'static,
    Kind::Event: Send + Clone + Debug,
    Subscription<Exchange, Instrument, Kind>:
        Identifier<Exchange::Channel> + Identifier<Exchange::Market>,
{
    let stream = init_boxed_stream(exchange, subscriptions, kind).await?;
    Ok(spawn_forward(stream, sender))
}

/// Initialize a [`MarketStream`] as a boxed, type-erased stream.
///
/// Creates a reconnecting market stream and returns it as [`BoxedMarketStream`]
/// for composition with other streams.
///
/// # Arguments
///
/// * `exchange` - The exchange instance
/// * `subscriptions` - Subscriptions (using `SubKind`) converted to specific `Kind`
/// * `kind` - The subscription kind marker
async fn init_boxed_stream<Exchange, Instrument, Kind>(
    exchange: Exchange,
    subscriptions: Vec<Subscription<ExchangeId, Instrument, SubKind>>,
    kind: Kind,
) -> Result<BoxedMarketStream<Instrument::Key, Kind::Event>, DataError>
where
    Exchange: StreamSelector<Instrument, Kind> + Clone + Send + 'static,
    Instrument: InstrumentData + Ord + Display + Send + Sync + 'static,
    Instrument::Key: Send + Sync + Debug + Clone,
    Kind: SubscriptionKind + Display + Copy + Send + Sync + 'static,
    Kind::Event: Send + Clone + Debug,
    Subscription<Exchange, Instrument, Kind>:
        Identifier<Exchange::Channel> + Identifier<Exchange::Market>,
{
    init_market_stream(
        STREAM_RECONNECTION_POLICY,
        subscriptions
            .into_iter()
            .map(|sub| Subscription::new(exchange.clone(), sub.instrument, kind))
            .collect(),
    )
    .await
    .map(|stream| stream.boxed())
}

/// Spawn a task that forwards events from a stream to a channel.
///
/// Runs until the stream is exhausted or receiver is dropped.
///
/// # Arguments
///
/// * `stream` - The market stream to read from
/// * `sender` - The channel sender to forward to
///
/// # Returns
///
/// [`JoinHandle`] for the spawned task.
fn spawn_forward<InstrumentKey, Event>(
    stream: BoxedMarketStream<InstrumentKey, Event>,
    sender: UnboundedTx<MarketStreamResult<InstrumentKey, Event>>,
) -> JoinHandle<()>
where
    InstrumentKey: Send + Sync + 'static + Debug + Clone,
    Event: Send + Clone + 'static + Debug,
{
    tokio::spawn(stream.forward_to(sender))
}

#[derive(Debug)]
pub struct DynamicStreams<InstrumentKey> {
    pub trades:
        VecMap<ExchangeId, UnboundedReceiverStream<MarketStreamResult<InstrumentKey, PublicTrade>>>,
    pub l1s:
        VecMap<ExchangeId, UnboundedReceiverStream<MarketStreamResult<InstrumentKey, OrderBookL1>>>,
    pub l2s: VecMap<
        ExchangeId,
        UnboundedReceiverStream<MarketStreamResult<InstrumentKey, OrderBookEvent>>,
    >,
    pub l3s: VecMap<
        ExchangeId,
        UnboundedReceiverStream<MarketStreamResult<InstrumentKey, OrderBookEvent>>,
    >,
    pub liquidations:
        VecMap<ExchangeId, UnboundedReceiverStream<MarketStreamResult<InstrumentKey, Liquidation>>>,
    pub candles:
        VecMap<ExchangeId, UnboundedReceiverStream<MarketStreamResult<InstrumentKey, Candle>>>,
}

impl<InstrumentKey> DynamicStreams<InstrumentKey> {
    /// Remove an exchange [`PublicTrade`] `Stream` from the [`DynamicStreams`] collection.
    ///
    /// Note that calling this method will permanently remove this `Stream` from [`Self`].
    pub fn select_trades(
        &mut self,
        exchange: ExchangeId,
    ) -> Option<UnboundedReceiverStream<MarketStreamResult<InstrumentKey, PublicTrade>>> {
        self.trades.remove(&exchange)
    }

    /// Select and merge every exchange [`PublicTrade`] `Stream` using
    /// [`SelectAll`](futures_util::stream::select_all::select_all).
    pub fn select_all_trades(
        &mut self,
    ) -> SelectAll<UnboundedReceiverStream<MarketStreamResult<InstrumentKey, PublicTrade>>> {
        futures_util::stream::select_all::select_all(std::mem::take(&mut self.trades).into_values())
    }

    /// Remove an exchange [`OrderBookL1`] `Stream` from the [`DynamicStreams`] collection.
    ///
    /// Note that calling this method will permanently remove this `Stream` from [`Self`].
    pub fn select_l1s(
        &mut self,
        exchange: ExchangeId,
    ) -> Option<UnboundedReceiverStream<MarketStreamResult<InstrumentKey, OrderBookL1>>> {
        self.l1s.remove(&exchange)
    }

    /// Select and merge every exchange [`OrderBookL1`] `Stream` using
    /// [`SelectAll`](futures_util::stream::select_all::select_all).
    pub fn select_all_l1s(
        &mut self,
    ) -> SelectAll<UnboundedReceiverStream<MarketStreamResult<InstrumentKey, OrderBookL1>>> {
        futures_util::stream::select_all::select_all(std::mem::take(&mut self.l1s).into_values())
    }

    /// Remove an exchange [`OrderBookEvent`] `Stream` from the [`DynamicStreams`] collection.
    ///
    /// Note that calling this method will permanently remove this `Stream` from [`Self`].
    pub fn select_l2s(
        &mut self,
        exchange: ExchangeId,
    ) -> Option<UnboundedReceiverStream<MarketStreamResult<InstrumentKey, OrderBookEvent>>> {
        self.l2s.remove(&exchange)
    }

    /// Select and merge every exchange [`OrderBookEvent`] `Stream` using
    /// [`SelectAll`](futures_util::stream::select_all::select_all).
    pub fn select_all_l2s(
        &mut self,
    ) -> SelectAll<UnboundedReceiverStream<MarketStreamResult<InstrumentKey, OrderBookEvent>>> {
        futures_util::stream::select_all::select_all(std::mem::take(&mut self.l2s).into_values())
    }

    /// Remove an exchange L3 [`OrderBookEvent`] `Stream` from the [`DynamicStreams`] collection.
    ///
    /// Note that calling this method will permanently remove this `Stream` from [`Self`].
    pub fn select_l3s(
        &mut self,
        exchange: ExchangeId,
    ) -> Option<UnboundedReceiverStream<MarketStreamResult<InstrumentKey, OrderBookEvent>>> {
        self.l3s.remove(&exchange)
    }

    /// Select and merge every exchange L3 [`OrderBookEvent`] `Stream` using
    /// [`SelectAll`](futures_util::stream::select_all::select_all).
    pub fn select_all_l3s(
        &mut self,
    ) -> SelectAll<UnboundedReceiverStream<MarketStreamResult<InstrumentKey, OrderBookEvent>>> {
        futures_util::stream::select_all::select_all(std::mem::take(&mut self.l3s).into_values())
    }

    /// Remove an exchange [`Liquidation`] `Stream` from the [`DynamicStreams`] collection.
    ///
    /// Note that calling this method will permanently remove this `Stream` from [`Self`].
    pub fn select_liquidations(
        &mut self,
        exchange: ExchangeId,
    ) -> Option<UnboundedReceiverStream<MarketStreamResult<InstrumentKey, Liquidation>>> {
        self.liquidations.remove(&exchange)
    }

    /// Select and merge every exchange [`Liquidation`] `Stream` using
    /// [`SelectAll`](futures_util::stream::select_all::select_all).
    pub fn select_all_liquidations(
        &mut self,
    ) -> SelectAll<UnboundedReceiverStream<MarketStreamResult<InstrumentKey, Liquidation>>> {
        futures_util::stream::select_all::select_all(
            std::mem::take(&mut self.liquidations).into_values(),
        )
    }

    /// Remove an exchange [`Candle`] `Stream` from the [`DynamicStreams`] collection.
    ///
    /// Note that calling this method will permanently remove this `Stream` from [`Self`].
    pub fn select_candles(
        &mut self,
        exchange: ExchangeId,
    ) -> Option<UnboundedReceiverStream<MarketStreamResult<InstrumentKey, Candle>>> {
        self.candles.remove(&exchange)
    }

    /// Select and merge every exchange [`Candle`] `Stream` using
    /// [`SelectAll`](futures_util::stream::select_all::select_all).
    pub fn select_all_candles(
        &mut self,
    ) -> SelectAll<UnboundedReceiverStream<MarketStreamResult<InstrumentKey, Candle>>> {
        futures_util::stream::select_all::select_all(std::mem::take(&mut self.candles).into_values())
    }

    /// Select and merge every exchange `Stream` for every data type using [`select_all`](futures_util::stream::select_all::select_all)
    ///
    /// Note that using [`MarketStreamResult<Instrument, DataKind>`] as the `Output` is suitable for most
    /// use cases.
    pub fn select_all<Output>(self) -> impl Stream<Item = Output>
    where
        InstrumentKey: Send + 'static,
        Output: 'static,
        MarketStreamResult<InstrumentKey, PublicTrade>: Into<Output>,
        MarketStreamResult<InstrumentKey, OrderBookL1>: Into<Output>,
        MarketStreamResult<InstrumentKey, OrderBookEvent>: Into<Output>,
        MarketStreamResult<InstrumentKey, Liquidation>: Into<Output>,
        MarketStreamResult<InstrumentKey, Candle>: Into<Output>,
    {
        let Self {
            trades,
            l1s,
            l2s,
            l3s,
            liquidations,
            candles,
        } = self;

        let trades = trades
            .into_values()
            .map(|stream| stream.map(MarketStreamResult::into).boxed());

        let l1s = l1s
            .into_values()
            .map(|stream| stream.map(MarketStreamResult::into).boxed());

        let l2s = l2s
            .into_values()
            .map(|stream| stream.map(MarketStreamResult::into).boxed());

        let l3s = l3s
            .into_values()
            .map(|stream| stream.map(MarketStreamResult::into).boxed());

        let liquidations = liquidations
            .into_values()
            .map(|stream| stream.map(MarketStreamResult::into).boxed());

        let candles = candles
            .into_values()
            .map(|stream| stream.map(MarketStreamResult::into).boxed());

        let all = trades
            .chain(l1s)
            .chain(l2s)
            .chain(l3s)
            .chain(liquidations)
            .chain(candles);

        futures_util::stream::select_all::select_all(all)
    }
}

pub fn validate_batches<SubBatchIter, SubIter, Sub, Instrument>(
    batches: SubBatchIter,
) -> Result<Vec<Vec<Subscription<ExchangeId, Instrument, SubKind>>>, DataError>
where
    SubBatchIter: IntoIterator<Item = SubIter>,
    SubIter: IntoIterator<Item = Sub>,
    Sub: Into<Subscription<ExchangeId, Instrument, SubKind>>,
    Instrument: InstrumentData + Ord,
{
    batches
        .into_iter()
        .map(validate_subscriptions::<SubIter, Sub, Instrument>)
        .collect()
}

pub fn validate_subscriptions<SubIter, Sub, Instrument>(
    batch: SubIter,
) -> Result<Vec<Subscription<ExchangeId, Instrument, SubKind>>, DataError>
where
    SubIter: IntoIterator<Item = Sub>,
    Sub: Into<Subscription<ExchangeId, Instrument, SubKind>>,
    Instrument: InstrumentData + Ord,
{
    // Validate Subscriptions
    let mut batch = batch
        .into_iter()
        .map(Sub::into)
        .map(Validator::validate)
        .collect::<Result<Vec<_>, SocketError>>()?;

    // Remove duplicate Subscriptions
    batch.sort();
    batch.dedup();

    Ok(batch)
}

struct Channels<InstrumentKey> {
    txs: Arc<Txs<InstrumentKey>>,
    rxs: Rxs<InstrumentKey>,
}

impl<'a, Instrument> TryFrom<&'a Vec<Vec<Subscription<ExchangeId, Instrument, SubKind>>>>
    for Channels<Instrument::Key>
where
    Instrument: InstrumentData,
{
    type Error = DataError;

    fn try_from(
        value: &'a Vec<Vec<Subscription<ExchangeId, Instrument, SubKind>>>,
    ) -> Result<Self, Self::Error> {
        let mut txs = Txs::default();
        let mut rxs = Rxs::default();

        // Track expected (exchange, kind) pairs
        let mut expected_pairs: HashSet<(ExchangeId, SubKind)> = HashSet::new();

        for sub in value.iter().flatten() {
            expected_pairs.insert((sub.exchange, sub.kind));

            match sub.kind {
                SubKind::PublicTrades => {
                    if let (None, None) =
                        (txs.trades.get(&sub.exchange), rxs.trades.get(&sub.exchange))
                    {
                        let (tx, rx) = mpsc_unbounded();
                        txs.trades.insert(sub.exchange, tx);
                        rxs.trades.insert(sub.exchange, rx);
                    }
                }
                SubKind::OrderBooksL1 => {
                    if let (None, None) = (txs.l1s.get(&sub.exchange), rxs.l1s.get(&sub.exchange)) {
                        let (tx, rx) = mpsc_unbounded();
                        txs.l1s.insert(sub.exchange, tx);
                        rxs.l1s.insert(sub.exchange, rx);
                    }
                }
                SubKind::OrderBooksL2 => {
                    if let (None, None) = (txs.l2s.get(&sub.exchange), rxs.l2s.get(&sub.exchange)) {
                        let (tx, rx) = mpsc_unbounded();
                        txs.l2s.insert(sub.exchange, tx);
                        rxs.l2s.insert(sub.exchange, rx);
                    }
                }
                SubKind::OrderBooksL3 => {
                    if let (None, None) = (txs.l3s.get(&sub.exchange), rxs.l3s.get(&sub.exchange)) {
                        let (tx, rx) = mpsc_unbounded();
                        txs.l3s.insert(sub.exchange, tx);
                        rxs.l3s.insert(sub.exchange, rx);
                    }
                }
                SubKind::Liquidations => {
                    if let (None, None) = (
                        txs.liquidations.get(&sub.exchange),
                        rxs.liquidations.get(&sub.exchange),
                    ) {
                        let (tx, rx) = mpsc_unbounded();
                        txs.liquidations.insert(sub.exchange, tx);
                        rxs.liquidations.insert(sub.exchange, rx);
                    }
                }
                SubKind::Candles => {
                    if let (None, None) =
                        (txs.candles.get(&sub.exchange), rxs.candles.get(&sub.exchange))
                    {
                        let (tx, rx) = mpsc_unbounded();
                        txs.candles.insert(sub.exchange, tx);
                        rxs.candles.insert(sub.exchange, rx);
                    }
                }
            }
        }

        // Validate all expected channels were created
        for (exchange, kind) in &expected_pairs {
            let exists = match kind {
                SubKind::PublicTrades => txs.trades.contains_key(exchange),
                SubKind::OrderBooksL1 => txs.l1s.contains_key(exchange),
                SubKind::OrderBooksL2 => txs.l2s.contains_key(exchange),
                SubKind::OrderBooksL3 => txs.l3s.contains_key(exchange),
                SubKind::Liquidations => txs.liquidations.contains_key(exchange),
                SubKind::Candles => txs.candles.contains_key(exchange),
            };

            if !exists {
                return Err(DataError::ChannelNotFound {
                    exchange: *exchange,
                    sub_kind: *kind,
                });
            }
        }

        Ok(Channels {
            txs: Arc::new(txs),
            rxs,
        })
    }
}

struct Txs<InstrumentKey> {
    trades: FnvHashMap<ExchangeId, UnboundedTx<MarketStreamResult<InstrumentKey, PublicTrade>>>,
    l1s: FnvHashMap<ExchangeId, UnboundedTx<MarketStreamResult<InstrumentKey, OrderBookL1>>>,
    l2s: FnvHashMap<ExchangeId, UnboundedTx<MarketStreamResult<InstrumentKey, OrderBookEvent>>>,
    l3s: FnvHashMap<ExchangeId, UnboundedTx<MarketStreamResult<InstrumentKey, OrderBookEvent>>>,
    liquidations:
        FnvHashMap<ExchangeId, UnboundedTx<MarketStreamResult<InstrumentKey, Liquidation>>>,
    candles: FnvHashMap<ExchangeId, UnboundedTx<MarketStreamResult<InstrumentKey, Candle>>>,
}

impl<InstrumentKey> Default for Txs<InstrumentKey> {
    fn default() -> Self {
        Self {
            trades: Default::default(),
            l1s: Default::default(),
            l2s: Default::default(),
            l3s: Default::default(),
            liquidations: Default::default(),
            candles: Default::default(),
        }
    }
}

struct Rxs<InstrumentKey> {
    trades: FnvHashMap<ExchangeId, UnboundedRx<MarketStreamResult<InstrumentKey, PublicTrade>>>,
    l1s: FnvHashMap<ExchangeId, UnboundedRx<MarketStreamResult<InstrumentKey, OrderBookL1>>>,
    l2s: FnvHashMap<ExchangeId, UnboundedRx<MarketStreamResult<InstrumentKey, OrderBookEvent>>>,
    l3s: FnvHashMap<ExchangeId, UnboundedRx<MarketStreamResult<InstrumentKey, OrderBookEvent>>>,
    liquidations:
        FnvHashMap<ExchangeId, UnboundedRx<MarketStreamResult<InstrumentKey, Liquidation>>>,
    candles: FnvHashMap<ExchangeId, UnboundedRx<MarketStreamResult<InstrumentKey, Candle>>>,
}

impl<InstrumentKey> Default for Rxs<InstrumentKey> {
    fn default() -> Self {
        Self {
            trades: Default::default(),
            l1s: Default::default(),
            l2s: Default::default(),
            l3s: Default::default(),
            liquidations: Default::default(),
            candles: Default::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use barter_instrument::instrument::market_data::{
        MarketDataInstrument, kind::MarketDataInstrumentKind,
    };

    fn test_instrument() -> MarketDataInstrument {
        MarketDataInstrument::new("btc", "usdt", MarketDataInstrumentKind::Spot)
    }

    #[test]
    fn test_channels_validation_passes_for_created_channels() {
        let subscriptions: Vec<Vec<Subscription<ExchangeId, MarketDataInstrument, SubKind>>> =
            vec![vec![
                Subscription::new(ExchangeId::BinanceSpot, test_instrument(), SubKind::PublicTrades),
            ]];

        let channels = Channels::<MarketDataInstrument>::try_from(&subscriptions);
        assert!(
            channels.is_ok(),
            "Expected channel creation to succeed, got: {:?}",
            channels.err()
        );

        let channels = channels.unwrap();
        // The validation happened during try_from, so if we got Ok, channels exist
        assert!(channels.txs.trades.contains_key(&ExchangeId::BinanceSpot));
    }

    #[test]
    fn test_channels_validation_passes_for_multiple_exchanges() {
        let subscriptions: Vec<Vec<Subscription<ExchangeId, MarketDataInstrument, SubKind>>> =
            vec![vec![
                Subscription::new(ExchangeId::BinanceSpot, test_instrument(), SubKind::PublicTrades),
                Subscription::new(ExchangeId::Coinbase, test_instrument(), SubKind::PublicTrades),
                Subscription::new(ExchangeId::BinanceSpot, test_instrument(), SubKind::OrderBooksL1),
            ]];

        let channels = Channels::<MarketDataInstrument>::try_from(&subscriptions);
        assert!(
            channels.is_ok(),
            "Expected channel creation to succeed, got: {:?}",
            channels.err()
        );

        let channels = channels.unwrap();
        // Verify all expected channels exist
        assert!(channels.txs.trades.contains_key(&ExchangeId::BinanceSpot));
        assert!(channels.txs.trades.contains_key(&ExchangeId::Coinbase));
        assert!(channels.txs.l1s.contains_key(&ExchangeId::BinanceSpot));
    }

    #[test]
    fn test_channels_validation_deduplicates_subscriptions() {
        // Same subscription repeated should only create one channel
        let subscriptions: Vec<Vec<Subscription<ExchangeId, MarketDataInstrument, SubKind>>> =
            vec![vec![
                Subscription::new(ExchangeId::BinanceSpot, test_instrument(), SubKind::PublicTrades),
                Subscription::new(ExchangeId::BinanceSpot, test_instrument(), SubKind::PublicTrades),
            ]];

        let channels = Channels::<MarketDataInstrument>::try_from(&subscriptions);
        assert!(
            channels.is_ok(),
            "Expected channel creation to succeed, got: {:?}",
            channels.err()
        );

        let channels = channels.unwrap();
        // Only one channel should be created despite duplicate subscriptions
        assert_eq!(channels.txs.trades.len(), 1);
        assert!(channels.txs.trades.contains_key(&ExchangeId::BinanceSpot));
    }

    #[test]
    fn test_channels_validation_with_all_sub_kinds() {
        let subscriptions: Vec<Vec<Subscription<ExchangeId, MarketDataInstrument, SubKind>>> =
            vec![vec![
                Subscription::new(ExchangeId::BinanceSpot, test_instrument(), SubKind::PublicTrades),
                Subscription::new(ExchangeId::BinanceSpot, test_instrument(), SubKind::OrderBooksL1),
                Subscription::new(ExchangeId::BinanceSpot, test_instrument(), SubKind::OrderBooksL2),
                Subscription::new(
                    ExchangeId::BinanceFuturesUsd,
                    test_instrument(),
                    SubKind::Liquidations,
                ),
            ]];

        let channels = Channels::<MarketDataInstrument>::try_from(&subscriptions);
        assert!(
            channels.is_ok(),
            "Expected channel creation to succeed, got: {:?}",
            channels.err()
        );

        let channels = channels.unwrap();
        assert!(channels.txs.trades.contains_key(&ExchangeId::BinanceSpot));
        assert!(channels.txs.l1s.contains_key(&ExchangeId::BinanceSpot));
        assert!(channels.txs.l2s.contains_key(&ExchangeId::BinanceSpot));
        assert!(channels
            .txs
            .liquidations
            .contains_key(&ExchangeId::BinanceFuturesUsd));
    }
}
