use crate::{
    AccountEventKind, InstrumentAccountSnapshot, UnindexedAccountEvent, UnindexedAccountSnapshot,
    balance::AssetBalance,
    client::mock::MockExecutionConfig,
    error::{ApiError, UnindexedApiError, UnindexedOrderError},
    exchange::mock::{
        account::AccountState,
        request::{MockExchangeRequest, MockExchangeRequestKind},
    },
    order::{
        Order, OrderEvent, OrderKey, OrderKind, TimeInForce, UnindexedOrder,
        id::{ClientOrderId, OrderId},
        request::{OrderRequestCancel, OrderRequestOpen},
        state::{Cancelled, Open},
    },
    trade::{AssetFees, Trade, TradeId},
};
use barter_data::subscription::book::OrderBookL1;
use barter_instrument::{
    Side,
    asset::{QuoteAsset, name::AssetNameExchange},
    exchange::ExchangeId,
    instrument::{Instrument, name::InstrumentNameExchange},
};
use barter_integration::snapshot::Snapshot;
use chrono::{DateTime, TimeDelta, Utc};
use fnv::FnvHashMap;
use futures::stream::BoxStream;
use itertools::Itertools;
use rust_decimal::Decimal;
use smol_str::ToSmolStr;
use std::fmt::Debug;
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio_stream::{StreamExt, wrappers::BroadcastStream};
use tracing::{error, info};

pub mod account;
pub mod request;

/// Market price update for triggering limit order fills.
///
/// The MockExchange receives these updates via a dedicated channel to know
/// the current best bid/ask for each instrument, enabling it to fill
/// resting limit orders when prices cross.
#[derive(Debug, Clone)]
pub struct MarketPriceUpdate {
    pub instrument: InstrumentNameExchange,
    pub best_bid: Decimal,
    pub best_ask: Decimal,
}

impl MarketPriceUpdate {
    /// Create a MarketPriceUpdate from an OrderBookL1, if both bid and ask are present.
    pub fn from_l1(instrument: InstrumentNameExchange, l1: &OrderBookL1) -> Option<Self> {
        let best_bid = l1.best_bid?.price;
        let best_ask = l1.best_ask?.price;
        Some(Self {
            instrument,
            best_bid,
            best_ask,
        })
    }
}

#[derive(Debug)]
pub struct MockExchange {
    pub exchange: ExchangeId,
    pub latency_ms: u64,
    pub taker_fees_percent: Decimal,
    pub maker_fees_percent: Decimal,
    pub request_rx: mpsc::UnboundedReceiver<MockExchangeRequest>,
    pub event_tx: broadcast::Sender<UnindexedAccountEvent>,
    pub instruments: FnvHashMap<InstrumentNameExchange, Instrument<ExchangeId, AssetNameExchange>>,
    pub account: AccountState,
    pub order_sequence: u64,
    pub time_exchange_latest: DateTime<Utc>,
    pub market_rx: mpsc::UnboundedReceiver<MarketPriceUpdate>,
    pub resting_orders: FnvHashMap<ClientOrderId, RestingOrder>,
    pub market_prices: FnvHashMap<InstrumentNameExchange, (Decimal, Decimal)>, // (best_bid, best_ask)
}

/// A limit order resting in the MockExchange's order book, waiting for
/// market prices to cross its limit price before filling.
#[derive(Debug, Clone)]
pub struct RestingOrder {
    pub instrument: InstrumentNameExchange,
    pub side: Side,
    pub price: Decimal,
    pub quantity: Decimal,
    pub kind: OrderKind,
    pub time_in_force: TimeInForce,
    pub strategy_id: crate::order::id::StrategyId,
    pub order_id: OrderId,
    pub key: OrderKey<ExchangeId, InstrumentNameExchange>,
    pub frozen_margin: Decimal,
}

impl MockExchange {
    pub fn new(
        config: MockExecutionConfig,
        request_rx: mpsc::UnboundedReceiver<MockExchangeRequest>,
        event_tx: broadcast::Sender<UnindexedAccountEvent>,
        instruments: FnvHashMap<InstrumentNameExchange, Instrument<ExchangeId, AssetNameExchange>>,
        market_rx: mpsc::UnboundedReceiver<MarketPriceUpdate>,
    ) -> Self {
        Self {
            exchange: config.mocked_exchange,
            latency_ms: config.latency_ms,
            taker_fees_percent: config.taker_fees_percent,
            maker_fees_percent: config.maker_fees_percent,
            request_rx,
            event_tx,
            instruments,
            account: AccountState::from(config.initial_state),
            order_sequence: 0,
            time_exchange_latest: Default::default(),
            market_rx,
            resting_orders: FnvHashMap::default(),
            market_prices: FnvHashMap::default(),
        }
    }

    pub async fn run(mut self) {
        loop {
            tokio::select! {
                request = self.request_rx.recv() => {
                    let Some(request) = request else { break };
                    self.update_time_exchange(request.time_request);
                    self.handle_request(request);
                }
                market_update = self.market_rx.recv() => {
                    let Some(update) = market_update else { break };
                    self.handle_market_update(update);
                }
            }
        }
        info!(exchange = %self.exchange, "MockExchange shutting down");
    }

    fn handle_request(&mut self, request: MockExchangeRequest) {
        match request.kind {
            MockExchangeRequestKind::FetchAccountSnapshot { response_tx } => {
                let snapshot = self.account_snapshot();
                self.respond_with_latency(response_tx, snapshot);
            }
            MockExchangeRequestKind::FetchBalances {
                response_tx,
                assets,
            } => {
                let balances = self
                    .account
                    .balances()
                    .filter(|balance| assets.contains(&balance.asset))
                    .cloned()
                    .collect();
                self.respond_with_latency(response_tx, balances);
            }
            MockExchangeRequestKind::FetchOrdersOpen {
                response_tx,
                instruments,
            } => {
                let orders_open = self
                    .account
                    .orders_open()
                    .filter(|order| instruments.contains(&order.key.instrument))
                    .cloned()
                    .collect();
                self.respond_with_latency(response_tx, orders_open);
            }
            MockExchangeRequestKind::FetchTrades {
                response_tx,
                time_since,
            } => {
                let trades = self.account.trades(time_since).cloned().collect();
                self.respond_with_latency(response_tx, trades);
            }
            MockExchangeRequestKind::CancelOrder {
                response_tx,
                request,
            } => {
                let order = self.cancel_order(request.clone());
                let response = OrderEvent {
                    key: order.key,
                    state: order.state,
                };
                self.respond_with_latency(response_tx, response);
            }
            MockExchangeRequestKind::OpenOrder {
                response_tx,
                request,
            } => {
                let (response, notifications) = self.open_order(request);
                self.respond_with_latency(response_tx, response);

                if let Some(notifications) = notifications {
                    self.account.ack_trade(notifications.trade.clone());
                    self.send_notifications_with_latency(notifications);
                }
            }
        }
    }

    fn handle_market_update(&mut self, update: MarketPriceUpdate) {
        // Update current market prices
        self.market_prices
            .insert(update.instrument, (update.best_bid, update.best_ask));
        // Limit order matching will be implemented in Task 5
        let _ = update; // Unused for now
    }

    fn update_time_exchange(&mut self, time_request: DateTime<Utc>) {
        let client_to_exchange_latency = self.latency_ms / 2;

        self.time_exchange_latest = time_request
            .checked_add_signed(TimeDelta::milliseconds(client_to_exchange_latency as i64))
            .unwrap_or(time_request);

        self.account.update_time_exchange(self.time_exchange_latest)
    }

    pub fn time_exchange(&self) -> DateTime<Utc> {
        self.time_exchange_latest
    }

    pub fn account_snapshot(&self) -> UnindexedAccountSnapshot {
        let balances = self.account.balances().cloned().collect();

        let orders_open = self
            .account
            .orders_open()
            .cloned()
            .map(UnindexedOrder::from);

        let orders_cancelled = self
            .account
            .orders_cancelled()
            .cloned()
            .map(UnindexedOrder::from);

        let orders_all = orders_open.chain(orders_cancelled);
        let orders_all = orders_all.sorted_unstable_by_key(|order| order.key.instrument.clone());
        let orders_by_instrument = orders_all.chunk_by(|order| order.key.instrument.clone());

        let instruments = orders_by_instrument
            .into_iter()
            .map(|(instrument, orders)| InstrumentAccountSnapshot {
                instrument,
                orders: orders.into_iter().collect(),
            })
            .collect();

        UnindexedAccountSnapshot {
            exchange: self.exchange,
            balances,
            instruments,
        }
    }

    /// Sends the provided `Response` via the [`oneshot::Sender`] after waiting for the latency
    /// [`Duration`].
    ///
    /// Used to simulate network latency between the exchange and client.
    fn respond_with_latency<Response>(
        &self,
        response_tx: oneshot::Sender<Response>,
        response: Response,
    ) where
        Response: Send + 'static,
    {
        let exchange = self.exchange;
        let latency = std::time::Duration::from_millis(self.latency_ms);

        tokio::spawn(async move {
            tokio::time::sleep(latency).await;
            if response_tx.send(response).is_err() {
                error!(
                    %exchange,
                    kind = std::any::type_name::<Response>(),
                    "MockExchange failed to send oneshot response to client"
                );
            }
        });
    }

    /// Sends the provided `OpenOrderNotifications` via the `MockExchanges`
    /// `broadcast::Sender<UnindexedAccountEvent>` after waiting for the latency
    /// [`Duration`].
    ///
    /// Used to simulate network latency between the exchange and client.
    fn send_notifications_with_latency(&self, notifications: OpenOrderNotifications) {
        let balance = self.build_account_event(notifications.balance);
        let trade = self.build_account_event(notifications.trade);

        let exchange = self.exchange;
        let latency = std::time::Duration::from_millis(self.latency_ms);
        let tx = self.event_tx.clone();
        tokio::spawn(async move {
            tokio::time::sleep(latency).await;

            if tx.send(balance).is_err() {
                error!(
                    %exchange,
                    kind = "Snapshot<AssetBalance<AssetNameExchange>",
                    "MockExchange failed to send AccountEvent notification to client"
                );
            }

            if tx.send(trade).is_err() {
                error!(
                    %exchange,
                    kind = "Trade<QuoteAsset, InstrumentNameExchange>",
                    "MockExchange failed to send AccountEvent notification to client"
                );
            }
        });
    }

    pub fn account_stream(&self) -> BoxStream<'static, UnindexedAccountEvent> {
        futures::StreamExt::boxed(BroadcastStream::new(self.event_tx.subscribe()).map_while(
            |result| match result {
                Ok(event) => Some(event),
                Err(error) => {
                    error!(
                        ?error,
                        "MockExchange Broadcast AccountStream lagged - terminating"
                    );
                    None
                }
            },
        ))
    }

    pub fn cancel_order(
        &mut self,
        _: OrderRequestCancel<ExchangeId, InstrumentNameExchange>,
    ) -> Order<ExchangeId, InstrumentNameExchange, Result<Cancelled, UnindexedOrderError>> {
        unimplemented!()
    }

    pub fn open_order(
        &mut self,
        request: OrderRequestOpen<ExchangeId, InstrumentNameExchange>,
    ) -> (
        Order<ExchangeId, InstrumentNameExchange, Result<Open, UnindexedOrderError>>,
        Option<OpenOrderNotifications>,
    ) {
        if let Err(error) = self.validate_order_kind_supported(request.state.kind) {
            return (build_open_order_err_response(request, error), None);
        }

        let underlying = match self.find_instrument_data(&request.key.instrument) {
            Ok(instrument) => instrument.underlying.clone(),
            Err(error) => return (build_open_order_err_response(request, error), None),
        };

        let time_exchange = self.time_exchange();

        let balance_change_result = match request.state.side {
            Side::Buy => {
                // Buying Instrument requires sufficient QuoteAsset Balance
                let current = self
                    .account
                    .balance_mut(&underlying.quote)
                    .expect("MockExchange has Balance for all configured Instrument assets");

                // Currently we only supported MarketKind orders, so they should be identical
                assert_eq!(current.balance.total, current.balance.free);

                let order_value_quote = request.state.price * request.state.quantity.abs();
                let order_fees_quote = order_value_quote * self.taker_fees_percent;
                let quote_required = order_value_quote + order_fees_quote;

                let maybe_new_balance = current.balance.free - quote_required;

                if maybe_new_balance >= Decimal::ZERO {
                    current.balance.free = maybe_new_balance;
                    current.balance.total = maybe_new_balance;
                    current.time_exchange = time_exchange;

                    Ok((current.clone(), AssetFees::quote_fees(order_fees_quote)))
                } else {
                    Err(ApiError::BalanceInsufficient(
                        underlying.quote,
                        format!(
                            "Available Balance: {}, Required Balance inc. fees: {}",
                            current.balance.free, quote_required
                        ),
                    ))
                }
            }
            Side::Sell => {
                // Selling Instrument requires sufficient BaseAsset Balance
                let current = self
                    .account
                    .balance_mut(&underlying.base)
                    .expect("MockExchange has Balance for all configured Instrument assets");

                // Currently we only supported MarketKind orders, so they should be identical
                assert_eq!(current.balance.total, current.balance.free);

                let order_value_base = request.state.quantity.abs();
                let order_fees_base = order_value_base * self.taker_fees_percent;
                let base_required = order_value_base + order_fees_base;

                let maybe_new_balance = current.balance.free - base_required;

                if maybe_new_balance >= Decimal::ZERO {
                    current.balance.free = maybe_new_balance;
                    current.balance.total = maybe_new_balance;
                    current.time_exchange = time_exchange;

                    let fees_quote = order_fees_base * request.state.price;

                    Ok((current.clone(), AssetFees::quote_fees(fees_quote)))
                } else {
                    Err(ApiError::BalanceInsufficient(
                        underlying.base,
                        format!(
                            "Available Balance: {}, Required Balance inc. fees: {}",
                            current.balance.free, base_required
                        ),
                    ))
                }
            }
        };

        let (balance_snapshot, fees) = match balance_change_result {
            Ok((balance_snapshot, fees)) => (Snapshot(balance_snapshot), fees),
            Err(error) => return (build_open_order_err_response(request, error), None),
        };

        let order_id = self.order_id_sequence_fetch_add();
        let trade_id = TradeId(order_id.0.clone());

        let order_response = Order {
            key: request.key.clone(),
            side: request.state.side,
            price: request.state.price,
            quantity: request.state.quantity,
            kind: request.state.kind,
            time_in_force: request.state.time_in_force,
            state: Ok(Open {
                id: order_id.clone(),
                time_exchange: self.time_exchange(),
                filled_quantity: request.state.quantity,
            }),
        };

        let notifications = OpenOrderNotifications {
            balance: balance_snapshot,
            trade: Trade {
                id: trade_id,
                order_id: order_id.clone(),
                instrument: request.key.instrument,
                strategy: request.key.strategy,
                time_exchange: self.time_exchange(),
                side: request.state.side,
                price: request.state.price,
                quantity: request.state.quantity,
                fees,
            },
        };

        (order_response, Some(notifications))
    }

    pub fn validate_order_kind_supported(
        &self,
        order_kind: OrderKind,
    ) -> Result<(), UnindexedOrderError> {
        if order_kind == OrderKind::Market {
            Ok(())
        } else {
            Err(UnindexedOrderError::Rejected(ApiError::OrderRejected(
                format!("MockExchange does not supported OrderKind: {order_kind}"),
            )))
        }
    }

    pub fn find_instrument_data(
        &self,
        instrument: &InstrumentNameExchange,
    ) -> Result<&Instrument<ExchangeId, AssetNameExchange>, UnindexedApiError> {
        self.instruments.get(instrument).ok_or_else(|| {
            ApiError::InstrumentInvalid(
                instrument.clone(),
                format!("MockExchange is not set-up for managing: {instrument}"),
            )
        })
    }

    fn order_id_sequence_fetch_add(&mut self) -> OrderId {
        let sequence = self.order_sequence;
        self.order_sequence += 1;
        OrderId::new(sequence.to_smolstr())
    }

    fn build_account_event<Kind>(&self, kind: Kind) -> UnindexedAccountEvent
    where
        Kind: Into<AccountEventKind<ExchangeId, AssetNameExchange, InstrumentNameExchange>>,
    {
        UnindexedAccountEvent {
            exchange: self.exchange,
            kind: kind.into(),
        }
    }
}

fn build_open_order_err_response<E>(
    request: OrderRequestOpen<ExchangeId, InstrumentNameExchange>,
    error: E,
) -> Order<ExchangeId, InstrumentNameExchange, Result<Open, UnindexedOrderError>>
where
    E: Into<UnindexedOrderError>,
{
    Order {
        key: request.key,
        side: request.state.side,
        price: request.state.price,
        quantity: request.state.quantity,
        kind: request.state.kind,
        time_in_force: request.state.time_in_force,
        state: Err(error.into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::balance::{AssetBalance, Balance};
    use crate::client::mock::MockExecutionConfig;
    use crate::order::request::RequestOpen;
    use crate::order::{OrderEvent, OrderKey};
    use barter_instrument::Underlying;
    use barter_instrument::asset::name::AssetNameExchange;
    use barter_instrument::instrument::kind::InstrumentKind;
    use barter_instrument::instrument::name::InstrumentNameExchange;

    fn mock_spot_instrument() -> Instrument<ExchangeId, AssetNameExchange> {
        use barter_instrument::instrument::name::InstrumentNameInternal;
        use barter_instrument::instrument::quote::InstrumentQuoteAsset;

        Instrument {
            exchange: ExchangeId::Deribit,
            name_internal: InstrumentNameInternal::from("btc-usd"),
            name_exchange: InstrumentNameExchange::from("BTC-USD"),
            underlying: Underlying {
                base: AssetNameExchange::from("btc"),
                quote: AssetNameExchange::from("usd"),
            },
            quote: InstrumentQuoteAsset::UnderlyingQuote,
            kind: InstrumentKind::Spot,
            spec: None,
        }
    }

    fn make_mock_exchange_spot(btc_balance: Decimal, usd_balance: Decimal) -> MockExchange {
        let (_request_tx, request_rx) = mpsc::unbounded_channel();
        let (event_tx, _event_rx) = broadcast::channel(16);
        let (_market_tx, market_rx) = mpsc::unbounded_channel();

        let mut instruments = FnvHashMap::default();
        let inst = mock_spot_instrument();
        instruments.insert(inst.name_exchange.clone(), inst);

        let snapshot = UnindexedAccountSnapshot {
            exchange: ExchangeId::Deribit,
            balances: vec![
                AssetBalance::new(
                    AssetNameExchange::from("btc"),
                    Balance::new(btc_balance, btc_balance),
                    Utc::now(),
                ),
                AssetBalance::new(
                    AssetNameExchange::from("usd"),
                    Balance::new(usd_balance, usd_balance),
                    Utc::now(),
                ),
            ],
            instruments: vec![],
        };

        let config = MockExecutionConfig::new(
            ExchangeId::Deribit,
            snapshot,
            10,
            Decimal::from(1) / Decimal::from(1000),  // taker
            Decimal::from(5) / Decimal::from(10000), // maker
        );

        MockExchange::new(config, request_rx, event_tx, instruments, market_rx)
    }

    fn make_open_request(
        side: Side,
        price: Decimal,
        quantity: Decimal,
    ) -> OrderRequestOpen<ExchangeId, InstrumentNameExchange> {
        OrderEvent {
            key: OrderKey {
                exchange: ExchangeId::Deribit,
                instrument: InstrumentNameExchange::from("BTC-USD"),
                strategy: crate::order::id::StrategyId::new("test"),
                cid: crate::order::id::ClientOrderId::random(),
            },
            state: RequestOpen {
                side,
                price,
                quantity,
                kind: OrderKind::Market,
                time_in_force: crate::order::TimeInForce::ImmediateOrCancel,
            },
        }
    }

    #[test]
    fn spot_sell_deducts_from_base_balance_not_quote() {
        // Arrange: 1.0 BTC, 0.0 USD — sell should succeed (we have base asset)
        let mut exchange = make_mock_exchange_spot(Decimal::from(1), Decimal::from(0));

        let request = make_open_request(
            Side::Sell,
            Decimal::from(50000),
            Decimal::from(5) / Decimal::from(10),
        );

        // Act
        let (response, notifications) = exchange.open_order(request);

        // Assert: should succeed (we have BTC to sell)
        assert!(
            response.state.is_ok(),
            "sell should succeed when base balance is sufficient"
        );
        assert!(
            notifications.is_some(),
            "should produce trade notifications"
        );

        // Verify BTC balance decreased
        let btc_balance = exchange
            .account
            .balance_mut(&AssetNameExchange::from("btc"))
            .unwrap();
        assert!(
            btc_balance.balance.free < Decimal::from(1),
            "BTC balance should have decreased"
        );
    }

    #[test]
    fn spot_sell_fails_when_base_balance_insufficient() {
        // Arrange: 0.0 BTC, 100000 USD — sell should fail (no base asset)
        let mut exchange = make_mock_exchange_spot(Decimal::from(0), Decimal::from(100000));

        let request = make_open_request(
            Side::Sell,
            Decimal::from(50000),
            Decimal::from(5) / Decimal::from(10),
        );

        // Act
        let (response, notifications) = exchange.open_order(request);

        // Assert: should fail (no BTC to sell)
        assert!(
            response.state.is_err(),
            "sell should fail when base balance is insufficient"
        );
        assert!(notifications.is_none());
    }

    #[test]
    fn market_price_update_from_l1_with_both_sides() {
        use barter_data::books::Level;
        use barter_data::subscription::book::OrderBookL1;

        let l1 = OrderBookL1::new(
            Utc::now(),
            Some(Level::new(Decimal::from(49000), Decimal::from(1))),
            Some(Level::new(Decimal::from(51000), Decimal::from(1))),
        );

        let update = MarketPriceUpdate::from_l1(InstrumentNameExchange::from("BTC-PERPETUAL"), &l1);

        assert!(update.is_some());
        let update = update.unwrap();
        assert_eq!(update.best_bid, Decimal::from(49000));
        assert_eq!(update.best_ask, Decimal::from(51000));
    }

    #[test]
    fn market_price_update_from_l1_missing_side_returns_none() {
        use barter_data::books::Level;
        use barter_data::subscription::book::OrderBookL1;

        let l1 = OrderBookL1::new(
            Utc::now(),
            Some(Level::new(Decimal::from(49000), Decimal::from(1))),
            None, // Missing ask
        );

        let update = MarketPriceUpdate::from_l1(InstrumentNameExchange::from("BTC-PERPETUAL"), &l1);

        assert!(update.is_none());
    }
}

#[derive(Debug)]
pub struct OpenOrderNotifications {
    pub balance: Snapshot<AssetBalance<AssetNameExchange>>,
    pub trade: Trade<QuoteAsset, InstrumentNameExchange>,
}
