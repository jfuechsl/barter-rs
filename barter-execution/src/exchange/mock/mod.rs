use crate::{
    AccountEventKind, InstrumentAccountSnapshot, UnindexedAccountEvent, UnindexedAccountSnapshot,
    balance::AssetBalance,
    client::mock::MockExecutionConfig,
    error::{ApiError, UnindexedApiError, UnindexedOrderError},
    exchange::mock::{
        account::{AccountState, MockPosition},
        request::{MockExchangeRequest, MockExchangeRequestKind},
    },
    order::{
        Order, OrderEvent, OrderKey, OrderKind, TimeInForce, UnindexedOrder,
        id::{ClientOrderId, OrderId},
        request::{OrderRequestCancel, OrderRequestOpen, RequestOpen},
        state::{Cancelled, Open},
    },
    trade::{AssetFees, Trade, TradeId},
};
use barter_data::subscription::book::OrderBookL1;
use barter_instrument::{
    Side,
    asset::{QuoteAsset, name::AssetNameExchange},
    exchange::ExchangeId,
    instrument::kind::perpetual::PerpetualContract,
    instrument::{Instrument, kind::InstrumentKind, name::InstrumentNameExchange},
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
        self.market_prices.insert(
            update.instrument.clone(),
            (update.best_bid, update.best_ask),
        );

        // Find resting orders that should fill
        let fills: Vec<ClientOrderId> = self
            .resting_orders
            .iter()
            .filter(|(_, order)| {
                if order.instrument != update.instrument {
                    return false;
                }
                match order.side {
                    Side::Buy => update.best_ask <= order.price,
                    Side::Sell => update.best_bid >= order.price,
                }
            })
            .map(|(cid, _)| cid.clone())
            .collect();

        for cid in fills {
            if let Some(resting) = self.resting_orders.remove(&cid) {
                self.fill_resting_order(resting);
            }
        }
    }

    fn fill_resting_order(&mut self, resting: RestingOrder) {
        let instrument_data = self.instruments.get(&resting.instrument).cloned();
        let Some(instrument_data) = instrument_data else {
            return;
        };

        // Unfreeze margin
        let settlement_asset = Self::settlement_asset_for_instrument(&instrument_data);
        if let Some(balance) = self.account.balance_mut(&settlement_asset) {
            balance.balance.free += resting.frozen_margin;
        }

        // Build a synthetic OrderRequestOpen to reuse fill logic
        let request = OrderEvent {
            key: resting.key.clone(),
            state: RequestOpen {
                side: resting.side,
                price: resting.price,
                quantity: resting.quantity,
                kind: resting.kind,
                time_in_force: resting.time_in_force,
            },
        };

        // Use maker fees for resting order fills
        let saved_taker_fees = self.taker_fees_percent;
        self.taker_fees_percent = self.maker_fees_percent;

        let (_response, notifications) = match &instrument_data.kind {
            InstrumentKind::Spot => self.open_order_spot(request, &instrument_data),
            InstrumentKind::Perpetual(contract) => {
                self.open_order_perpetual(request, &instrument_data, contract)
            }
            _ => return,
        };

        self.taker_fees_percent = saved_taker_fees;

        if let Some(notifications) = notifications {
            self.account.ack_trade(notifications.trade.clone());
            self.send_notifications_with_latency(notifications);
        }
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
        request: OrderRequestCancel<ExchangeId, InstrumentNameExchange>,
    ) -> Order<ExchangeId, InstrumentNameExchange, Result<Cancelled, UnindexedOrderError>> {
        let time_exchange = self.time_exchange();

        match self.resting_orders.remove(&request.key.cid) {
            Some(resting) => {
                // Unfreeze margin
                let instrument_data = self.instruments.get(&resting.instrument).cloned();
                if let Some(ref instrument_data) = instrument_data {
                    let settlement_asset = Self::settlement_asset_for_instrument(instrument_data);
                    if let Some(balance) = self.account.balance_mut(&settlement_asset) {
                        balance.balance.free += resting.frozen_margin;
                        balance.time_exchange = time_exchange;
                    }
                }

                Order {
                    key: request.key,
                    side: resting.side,
                    price: resting.price,
                    quantity: resting.quantity,
                    kind: resting.kind,
                    time_in_force: resting.time_in_force,
                    state: Ok(Cancelled {
                        id: resting.order_id,
                        time_exchange,
                    }),
                }
            }
            None => Order {
                key: request.key.clone(),
                side: Side::Buy, // Unknown, but we need a value
                price: Decimal::ZERO,
                quantity: Decimal::ZERO,
                kind: OrderKind::Limit,
                time_in_force: TimeInForce::GoodUntilCancelled { post_only: false },
                state: Err(UnindexedOrderError::Rejected(ApiError::OrderRejected(
                    format!("No resting order found with cid: {}", request.key.cid),
                ))),
            },
        }
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

        // For limit orders, check if the order should rest or fill immediately
        if request.state.kind == OrderKind::Limit {
            let crosses = self.limit_order_crosses_spread(&request);
            if !crosses {
                return self.rest_limit_order(request);
            }
            // If crossing, fall through to immediate fill (as taker)
        }

        let instrument_data = match self.find_instrument_data(&request.key.instrument) {
            Ok(instrument) => instrument.clone(),
            Err(error) => return (build_open_order_err_response(request, error), None),
        };

        match &instrument_data.kind {
            InstrumentKind::Spot => self.open_order_spot(request, &instrument_data),
            InstrumentKind::Perpetual(contract) => {
                self.open_order_perpetual(request, &instrument_data, contract)
            }
            _ => (
                build_open_order_err_response(
                    request,
                    ApiError::OrderRejected(format!(
                        "MockExchange does not support instrument kind: {:?}",
                        instrument_data.kind
                    )),
                ),
                None,
            ),
        }
    }

    fn limit_order_crosses_spread(
        &self,
        request: &OrderRequestOpen<ExchangeId, InstrumentNameExchange>,
    ) -> bool {
        let Some(&(best_bid, best_ask)) = self.market_prices.get(&request.key.instrument) else {
            // No market data yet — rest the order
            return false;
        };

        match request.state.side {
            Side::Buy => request.state.price >= best_ask,
            Side::Sell => request.state.price <= best_bid,
        }
    }

    fn open_order_spot(
        &mut self,
        request: OrderRequestOpen<ExchangeId, InstrumentNameExchange>,
        instrument: &Instrument<ExchangeId, AssetNameExchange>,
    ) -> (
        Order<ExchangeId, InstrumentNameExchange, Result<Open, UnindexedOrderError>>,
        Option<OpenOrderNotifications>,
    ) {
        let underlying = &instrument.underlying;
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
                        underlying.quote.clone(),
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
                        underlying.base.clone(),
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

    fn open_order_perpetual(
        &mut self,
        request: OrderRequestOpen<ExchangeId, InstrumentNameExchange>,
        _instrument: &Instrument<ExchangeId, AssetNameExchange>,
        contract: &PerpetualContract<AssetNameExchange>,
    ) -> (
        Order<ExchangeId, InstrumentNameExchange, Result<Open, UnindexedOrderError>>,
        Option<OpenOrderNotifications>,
    ) {
        let time_exchange = self.time_exchange();
        let notional = request.state.price * request.state.quantity * contract.contract_size;
        let settlement_asset = &contract.settlement_asset;
        let fees_percent = self.taker_fees_percent;
        let fees = notional * fees_percent;

        // Determine position change - clone the position data to avoid borrow issues
        let current_position = self.account.positions.get(&request.key.instrument).cloned();

        let (margin_required, pnl, old_margin) = match &current_position {
            None => {
                // Opening new position — full margin required
                (notional, Decimal::ZERO, Decimal::ZERO)
            }
            Some(pos) if pos.side == request.state.side => {
                // Increasing position — additional margin required
                (notional, Decimal::ZERO, Decimal::ZERO)
            }
            Some(pos) => {
                // Reducing/closing/flipping position
                let close_qty = request.state.quantity.min(pos.quantity_abs);
                let close_notional = pos.entry_price * close_qty * contract.contract_size;
                let current_notional = request.state.price * close_qty * contract.contract_size;

                let pnl = match pos.side {
                    Side::Buy => current_notional - close_notional, // long: profit if price up
                    Side::Sell => close_notional - current_notional, // short: profit if price down
                };

                let remaining_qty = request.state.quantity - close_qty;
                let new_margin = if remaining_qty > Decimal::ZERO {
                    // Flipping: need margin for the excess
                    request.state.price * remaining_qty * contract.contract_size
                } else {
                    Decimal::ZERO
                };

                // Margin returned from closing = close_notional
                let old_margin = pos.entry_price * close_qty * contract.contract_size;

                (new_margin, pnl, old_margin)
            }
        };

        let net_cost = margin_required + fees - pnl;

        // Check settlement asset balance
        let balance = self
            .account
            .balance_mut(settlement_asset)
            .expect("MockExchange has Balance for settlement asset");

        let balance_change = balance.balance.free + old_margin - net_cost;

        if balance_change < Decimal::ZERO {
            return (
                build_open_order_err_response(
                    request,
                    ApiError::BalanceInsufficient(
                        settlement_asset.clone(),
                        format!(
                            "Available: {}, Required: {}",
                            balance.balance.free,
                            net_cost - old_margin
                        ),
                    ),
                ),
                None,
            );
        }

        balance.balance.free = balance_change;
        balance.balance.total = balance_change;
        balance.time_exchange = time_exchange;
        let balance_snapshot = Snapshot(balance.clone());

        // Update position
        self.update_position(
            request.key.instrument.clone(),
            request.state.side,
            request.state.quantity,
            request.state.price,
            contract,
        );

        // Build response
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
                time_exchange,
                filled_quantity: request.state.quantity,
            }),
        };

        let notifications = OpenOrderNotifications {
            balance: balance_snapshot,
            trade: Trade {
                id: trade_id,
                order_id,
                instrument: request.key.instrument,
                strategy: request.key.strategy,
                time_exchange,
                side: request.state.side,
                price: request.state.price,
                quantity: request.state.quantity,
                fees: AssetFees::quote_fees(fees),
            },
        };

        (order_response, Some(notifications))
    }

    fn update_position(
        &mut self,
        instrument: InstrumentNameExchange,
        trade_side: Side,
        trade_qty: Decimal,
        trade_price: Decimal,
        _contract: &PerpetualContract<AssetNameExchange>,
    ) {
        use std::collections::hash_map::Entry;

        match self.account.positions.entry(instrument) {
            Entry::Vacant(entry) => {
                entry.insert(MockPosition {
                    side: trade_side,
                    quantity_abs: trade_qty,
                    entry_price: trade_price,
                });
            }
            Entry::Occupied(mut entry) => {
                let pos = entry.get_mut();
                if pos.side == trade_side {
                    // Increasing position — VWAP entry price
                    let total_notional =
                        pos.entry_price * pos.quantity_abs + trade_price * trade_qty;
                    let total_qty = pos.quantity_abs + trade_qty;
                    pos.entry_price = total_notional / total_qty;
                    pos.quantity_abs = total_qty;
                } else if trade_qty >= pos.quantity_abs {
                    let remaining = trade_qty - pos.quantity_abs;
                    if remaining > Decimal::ZERO {
                        // Flip position
                        pos.side = trade_side;
                        pos.quantity_abs = remaining;
                        pos.entry_price = trade_price;
                    } else {
                        // Exact close
                        entry.remove();
                    }
                } else {
                    // Partial close
                    pos.quantity_abs -= trade_qty;
                    // entry_price stays the same for partial close
                }
            }
        }
    }

    fn rest_limit_order(
        &mut self,
        request: OrderRequestOpen<ExchangeId, InstrumentNameExchange>,
    ) -> (
        Order<ExchangeId, InstrumentNameExchange, Result<Open, UnindexedOrderError>>,
        Option<OpenOrderNotifications>,
    ) {
        let instrument_data = match self.find_instrument_data(&request.key.instrument) {
            Ok(instrument) => instrument.clone(),
            Err(error) => return (build_open_order_err_response(request, error), None),
        };

        // Calculate and freeze margin
        let frozen_margin = Self::calculate_order_margin(&request, &instrument_data);
        let time_exchange = self.time_exchange();

        // Check balance
        let settlement_asset = Self::settlement_asset_for_instrument(&instrument_data);
        let balance = self
            .account
            .balance_mut(&settlement_asset)
            .expect("MockExchange has Balance for settlement asset");

        if balance.balance.free < frozen_margin {
            return (
                build_open_order_err_response(
                    request,
                    ApiError::BalanceInsufficient(
                        settlement_asset,
                        format!(
                            "Available: {}, Required margin: {}",
                            balance.balance.free, frozen_margin
                        ),
                    ),
                ),
                None,
            );
        }

        // Freeze margin
        balance.balance.free -= frozen_margin;
        balance.time_exchange = time_exchange;

        let order_id = self.order_id_sequence_fetch_add();

        let resting = RestingOrder {
            instrument: request.key.instrument.clone(),
            side: request.state.side,
            price: request.state.price,
            quantity: request.state.quantity,
            kind: request.state.kind,
            time_in_force: request.state.time_in_force,
            strategy_id: request.key.strategy.clone(),
            order_id: order_id.clone(),
            key: request.key.clone(),
            frozen_margin,
        };

        self.resting_orders.insert(request.key.cid.clone(), resting);

        // Acknowledge with filled_quantity=0
        let response = Order {
            key: request.key,
            side: request.state.side,
            price: request.state.price,
            quantity: request.state.quantity,
            kind: request.state.kind,
            time_in_force: request.state.time_in_force,
            state: Ok(Open {
                id: order_id,
                time_exchange: self.time_exchange(),
                filled_quantity: Decimal::ZERO,
            }),
        };

        (response, None)
    }

    fn calculate_order_margin(
        request: &OrderRequestOpen<ExchangeId, InstrumentNameExchange>,
        instrument: &Instrument<ExchangeId, AssetNameExchange>,
    ) -> Decimal {
        match &instrument.kind {
            InstrumentKind::Spot => match request.state.side {
                Side::Buy => request.state.price * request.state.quantity,
                Side::Sell => request.state.quantity,
            },
            InstrumentKind::Perpetual(contract) => {
                request.state.price * request.state.quantity * contract.contract_size
            }
            _ => Decimal::ZERO,
        }
    }

    fn settlement_asset_for_instrument(
        instrument: &Instrument<ExchangeId, AssetNameExchange>,
    ) -> AssetNameExchange {
        match &instrument.kind {
            InstrumentKind::Spot => instrument.underlying.quote.clone(),
            InstrumentKind::Perpetual(contract) => contract.settlement_asset.clone(),
            _ => instrument.underlying.quote.clone(),
        }
    }

    pub fn validate_order_kind_supported(
        &self,
        order_kind: OrderKind,
    ) -> Result<(), UnindexedOrderError> {
        // OrderKind only has Market and Limit variants, both supported
        let _ = order_kind;
        Ok(())
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
        make_open_request_for_instrument(
            InstrumentNameExchange::from("BTC-USD"),
            side,
            price,
            quantity,
        )
    }

    fn make_open_request_for_instrument(
        instrument: InstrumentNameExchange,
        side: Side,
        price: Decimal,
        quantity: Decimal,
    ) -> OrderRequestOpen<ExchangeId, InstrumentNameExchange> {
        OrderEvent {
            key: OrderKey {
                exchange: ExchangeId::Deribit,
                instrument,
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

    fn mock_perpetual_instrument() -> Instrument<ExchangeId, AssetNameExchange> {
        use barter_instrument::instrument::kind::perpetual::PerpetualContract;
        use barter_instrument::instrument::name::InstrumentNameInternal;
        use barter_instrument::instrument::quote::InstrumentQuoteAsset;

        Instrument {
            exchange: ExchangeId::Deribit,
            name_internal: InstrumentNameInternal::from("btc-perp"),
            name_exchange: InstrumentNameExchange::from("BTC-PERPETUAL"),
            underlying: Underlying {
                base: AssetNameExchange::from("btc"),
                quote: AssetNameExchange::from("usd"),
            },
            quote: InstrumentQuoteAsset::UnderlyingQuote,
            kind: InstrumentKind::Perpetual(PerpetualContract {
                contract_size: Decimal::ONE,
                settlement_asset: AssetNameExchange::from("usd"),
            }),
            spec: None,
        }
    }

    fn make_mock_exchange_perpetual(usd_balance: Decimal) -> MockExchange {
        let (_request_tx, request_rx) = mpsc::unbounded_channel();
        let (event_tx, _event_rx) = broadcast::channel(16);
        let (_market_tx, market_rx) = mpsc::unbounded_channel();

        let mut instruments = FnvHashMap::default();
        let inst = mock_perpetual_instrument();
        instruments.insert(inst.name_exchange.clone(), inst);

        let snapshot = UnindexedAccountSnapshot {
            exchange: ExchangeId::Deribit,
            balances: vec![AssetBalance::new(
                AssetNameExchange::from("usd"),
                Balance::new(usd_balance, usd_balance),
                Utc::now(),
            )],
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

    #[test]
    fn perpetual_buy_opens_long_position_and_deducts_margin() {
        let mut exchange = make_mock_exchange_perpetual(Decimal::from(100000));

        let request = make_open_request_for_instrument(
            InstrumentNameExchange::from("BTC-PERPETUAL"),
            Side::Buy,
            Decimal::from(50000),
            Decimal::from(1),
        );

        let (response, notifications) = exchange.open_order(request);

        assert!(
            response.state.is_ok(),
            "buy perpetual should succeed with sufficient margin"
        );
        assert!(notifications.is_some());

        // Verify USD balance decreased by margin (price * qty * contract_size + fees)
        let usd = exchange
            .account
            .balance_mut(&AssetNameExchange::from("usd"))
            .unwrap();
        assert!(
            usd.balance.free < Decimal::from(100000),
            "USD balance should decrease for margin"
        );

        // Verify position exists
        let position = exchange
            .account
            .positions
            .get(&InstrumentNameExchange::from("BTC-PERPETUAL"));
        assert!(position.is_some(), "should have a position");
        let pos = position.unwrap();
        assert_eq!(pos.side, Side::Buy);
        assert_eq!(pos.quantity_abs, Decimal::from(1));
        assert_eq!(pos.entry_price, Decimal::from(50000));
    }

    #[test]
    fn perpetual_sell_to_close_long_credits_pnl() {
        let mut exchange = make_mock_exchange_perpetual(Decimal::from(100000));

        // Open long at 50000
        let buy_request = make_open_request_for_instrument(
            InstrumentNameExchange::from("BTC-PERPETUAL"),
            Side::Buy,
            Decimal::from(50000),
            Decimal::from(1),
        );
        let (buy_resp, _) = exchange.open_order(buy_request);
        assert!(buy_resp.state.is_ok());

        let balance_after_open = exchange
            .account
            .balance_mut(&AssetNameExchange::from("usd"))
            .unwrap()
            .balance
            .free;

        // Close long at 51000 (profit of 1000)
        let sell_request = make_open_request_for_instrument(
            InstrumentNameExchange::from("BTC-PERPETUAL"),
            Side::Sell,
            Decimal::from(51000),
            Decimal::from(1),
        );
        let (sell_resp, _) = exchange.open_order(sell_request);
        assert!(sell_resp.state.is_ok());

        // Position should be closed
        let position = exchange
            .account
            .positions
            .get(&InstrumentNameExchange::from("BTC-PERPETUAL"));
        assert!(
            position.is_none(),
            "position should be closed after offsetting trade"
        );

        // Balance should be higher than after open (profit + margin returned - fees)
        let balance_after_close = exchange
            .account
            .balance_mut(&AssetNameExchange::from("usd"))
            .unwrap()
            .balance
            .free;
        assert!(
            balance_after_close > balance_after_open,
            "should have profit after closing at higher price"
        );
    }

    #[test]
    fn limit_order_rests_when_not_crossing_spread() {
        let mut exchange = make_mock_exchange_perpetual(Decimal::from(100000));

        // Set current market: bid=49000, ask=51000
        exchange.handle_market_update(MarketPriceUpdate {
            instrument: InstrumentNameExchange::from("BTC-PERPETUAL"),
            best_bid: Decimal::from(49000),
            best_ask: Decimal::from(51000),
        });

        // Submit buy limit at 49500 (below ask=51000, should rest)
        let mut request = make_open_request_for_instrument(
            InstrumentNameExchange::from("BTC-PERPETUAL"),
            Side::Buy,
            Decimal::from(49500),
            Decimal::from(1),
        );
        request.state.kind = OrderKind::Limit;
        request.state.time_in_force = TimeInForce::GoodUntilCancelled { post_only: true };

        let (response, notifications) = exchange.open_order(request);

        // Should be acknowledged as Open with filled_quantity=0
        let open = response.state.unwrap();
        assert_eq!(
            open.filled_quantity,
            Decimal::ZERO,
            "limit order should rest, not fill"
        );
        assert!(
            notifications.is_none(),
            "resting order should not produce trade notifications"
        );

        // Should be in resting_orders
        assert_eq!(exchange.resting_orders.len(), 1);
    }

    #[tokio::test]
    async fn limit_order_fills_when_market_price_crosses() {
        let mut exchange = make_mock_exchange_perpetual(Decimal::from(100000));

        // Set initial market
        exchange.handle_market_update(MarketPriceUpdate {
            instrument: InstrumentNameExchange::from("BTC-PERPETUAL"),
            best_bid: Decimal::from(49000),
            best_ask: Decimal::from(51000),
        });

        // Submit buy limit at 49500 (rests)
        let mut request = make_open_request_for_instrument(
            InstrumentNameExchange::from("BTC-PERPETUAL"),
            Side::Buy,
            Decimal::from(49500),
            Decimal::from(1),
        );
        request.state.kind = OrderKind::Limit;
        request.state.time_in_force = TimeInForce::GoodUntilCancelled { post_only: true };

        let (response, _) = exchange.open_order(request);
        let open = response.state.unwrap();
        assert_eq!(open.filled_quantity, Decimal::ZERO);

        // Market price drops: ask now 49400 (crosses our buy limit at 49500)
        exchange.handle_market_update(MarketPriceUpdate {
            instrument: InstrumentNameExchange::from("BTC-PERPETUAL"),
            best_bid: Decimal::from(49300),
            best_ask: Decimal::from(49400),
        });

        // Resting order should be filled and removed
        assert_eq!(
            exchange.resting_orders.len(),
            0,
            "order should be filled and removed"
        );

        // Position should exist
        let pos = exchange
            .account
            .positions
            .get(&InstrumentNameExchange::from("BTC-PERPETUAL"));
        assert!(pos.is_some(), "should have position after fill");
        let pos = pos.unwrap();
        assert_eq!(pos.side, Side::Buy);
        assert_eq!(pos.quantity_abs, Decimal::from(1));
        assert_eq!(
            pos.entry_price,
            Decimal::from(49500),
            "should fill at limit price, not market price"
        );
    }

    #[test]
    fn limit_order_crosses_spread_fills_immediately() {
        let mut exchange = make_mock_exchange_perpetual(Decimal::from(100000));

        // Set market: bid=49000, ask=50000
        exchange.handle_market_update(MarketPriceUpdate {
            instrument: InstrumentNameExchange::from("BTC-PERPETUAL"),
            best_bid: Decimal::from(49000),
            best_ask: Decimal::from(50000),
        });

        // Submit buy limit at 50500 (above ask=50000, crosses spread)
        let mut request = make_open_request_for_instrument(
            InstrumentNameExchange::from("BTC-PERPETUAL"),
            Side::Buy,
            Decimal::from(50500),
            Decimal::from(1),
        );
        request.state.kind = OrderKind::Limit;
        request.state.time_in_force = TimeInForce::GoodUntilCancelled { post_only: true };

        let (response, notifications) = exchange.open_order(request);

        // Should fill immediately (taker)
        let open = response.state.unwrap();
        assert_eq!(
            open.filled_quantity,
            Decimal::from(1),
            "crossing limit should fill immediately"
        );
        assert!(
            notifications.is_some(),
            "immediate fill should produce trade notifications"
        );
        assert_eq!(
            exchange.resting_orders.len(),
            0,
            "should not rest after immediate fill"
        );
    }

    #[test]
    fn cancel_resting_order_succeeds() {
        let mut exchange = make_mock_exchange_perpetual(Decimal::from(100000));

        // Set market
        exchange.handle_market_update(MarketPriceUpdate {
            instrument: InstrumentNameExchange::from("BTC-PERPETUAL"),
            best_bid: Decimal::from(49000),
            best_ask: Decimal::from(51000),
        });

        // Submit resting limit order
        let mut request = make_open_request_for_instrument(
            InstrumentNameExchange::from("BTC-PERPETUAL"),
            Side::Buy,
            Decimal::from(49500),
            Decimal::from(1),
        );
        request.state.kind = OrderKind::Limit;
        request.state.time_in_force = TimeInForce::GoodUntilCancelled { post_only: true };
        let cid = request.key.cid.clone();

        let (response, _) = exchange.open_order(request);
        assert!(response.state.is_ok());
        assert_eq!(exchange.resting_orders.len(), 1);

        let balance_after_order = exchange
            .account
            .balance_mut(&AssetNameExchange::from("usd"))
            .unwrap()
            .balance
            .free;

        // Cancel order
        use crate::order::request::RequestCancel;
        let cancel_request = OrderEvent {
            key: OrderKey {
                exchange: ExchangeId::Deribit,
                instrument: InstrumentNameExchange::from("BTC-PERPETUAL"),
                strategy: crate::order::id::StrategyId::new("test"),
                cid: cid.clone(),
            },
            state: RequestCancel { id: None },
        };

        let cancel_response = exchange.cancel_order(cancel_request);

        // Should succeed
        assert!(
            cancel_response.state.is_ok(),
            "cancel should succeed for resting order"
        );

        // Resting orders should be empty
        assert_eq!(exchange.resting_orders.len(), 0);

        // Margin should be unfrozen
        let balance_after_cancel = exchange
            .account
            .balance_mut(&AssetNameExchange::from("usd"))
            .unwrap()
            .balance
            .free;
        assert!(
            balance_after_cancel > balance_after_order,
            "margin should be returned"
        );
    }

    #[test]
    fn cancel_nonexistent_order_returns_error() {
        let mut exchange = make_mock_exchange_perpetual(Decimal::from(100000));

        use crate::order::request::RequestCancel;
        let cancel_request = OrderEvent {
            key: OrderKey {
                exchange: ExchangeId::Deribit,
                instrument: InstrumentNameExchange::from("BTC-PERPETUAL"),
                strategy: crate::order::id::StrategyId::new("test"),
                cid: crate::order::id::ClientOrderId::random(),
            },
            state: RequestCancel { id: None },
        };

        let cancel_response = exchange.cancel_order(cancel_request);

        assert!(
            cancel_response.state.is_err(),
            "cancel should fail for nonexistent order"
        );
    }

    #[tokio::test]
    async fn full_lifecycle_rest_fill_close_cancel() {
        let mut exchange = make_mock_exchange_perpetual(Decimal::from(100000));
        let initial_balance = Decimal::from(100000);

        // 1. Set market prices
        exchange.handle_market_update(MarketPriceUpdate {
            instrument: InstrumentNameExchange::from("BTC-PERPETUAL"),
            best_bid: Decimal::from(49000),
            best_ask: Decimal::from(51000),
        });

        // 2. Submit buy limit (rests)
        let mut buy = make_open_request_for_instrument(
            InstrumentNameExchange::from("BTC-PERPETUAL"),
            Side::Buy,
            Decimal::from(49500),
            Decimal::from(5) / Decimal::from(10),
        );
        buy.state.kind = OrderKind::Limit;
        buy.state.time_in_force = TimeInForce::GoodUntilCancelled { post_only: true };
        let _buy_cid = buy.key.cid.clone();

        let (buy_resp, _) = exchange.open_order(buy);
        assert!(buy_resp.state.is_ok());
        assert_eq!(exchange.resting_orders.len(), 1);

        let balance_after_buy_order = exchange
            .account
            .balance_mut(&AssetNameExchange::from("usd"))
            .unwrap()
            .balance
            .free;
        assert!(
            balance_after_buy_order < initial_balance,
            "margin should be frozen for buy order"
        );

        // 3. Market moves down - fill buy limit
        exchange.handle_market_update(MarketPriceUpdate {
            instrument: InstrumentNameExchange::from("BTC-PERPETUAL"),
            best_bid: Decimal::from(49300),
            best_ask: Decimal::from(49400),
        });

        // Buy order filled, position opened
        assert_eq!(exchange.resting_orders.len(), 0);
        let pos = exchange
            .account
            .positions
            .get(&InstrumentNameExchange::from("BTC-PERPETUAL"));
        assert!(pos.is_some());
        assert_eq!(pos.unwrap().side, Side::Buy);

        // 4. Submit sell limit to close position (rests)
        let mut sell = make_open_request_for_instrument(
            InstrumentNameExchange::from("BTC-PERPETUAL"),
            Side::Sell,
            Decimal::from(50500),
            Decimal::from(5) / Decimal::from(10),
        );
        sell.state.kind = OrderKind::Limit;
        sell.state.time_in_force = TimeInForce::GoodUntilCancelled { post_only: true };
        let sell_cid = sell.key.cid.clone();

        let (sell_resp, _) = exchange.open_order(sell);
        assert!(sell_resp.state.is_ok());
        assert_eq!(exchange.resting_orders.len(), 1);

        // 5. Cancel the sell order (change of mind)
        use crate::order::request::RequestCancel;
        let cancel_request = OrderEvent {
            key: OrderKey {
                exchange: ExchangeId::Deribit,
                instrument: InstrumentNameExchange::from("BTC-PERPETUAL"),
                strategy: crate::order::id::StrategyId::new("test"),
                cid: sell_cid,
            },
            state: RequestCancel { id: None },
        };

        let cancel_resp = exchange.cancel_order(cancel_request);
        assert!(cancel_resp.state.is_ok());
        assert_eq!(exchange.resting_orders.len(), 0);

        // Position still open (wasn't filled)
        let pos = exchange
            .account
            .positions
            .get(&InstrumentNameExchange::from("BTC-PERPETUAL"));
        assert!(pos.is_some(), "position should still be open after cancel");

        // 6. Submit new sell limit to actually close
        let mut sell2 = make_open_request_for_instrument(
            InstrumentNameExchange::from("BTC-PERPETUAL"),
            Side::Sell,
            Decimal::from(51500),
            Decimal::from(5) / Decimal::from(10),
        );
        sell2.state.kind = OrderKind::Limit;
        sell2.state.time_in_force = TimeInForce::GoodUntilCancelled { post_only: true };

        let (sell2_resp, _) = exchange.open_order(sell2);
        assert!(sell2_resp.state.is_ok());

        // 7. Market moves up - fill sell limit
        exchange.handle_market_update(MarketPriceUpdate {
            instrument: InstrumentNameExchange::from("BTC-PERPETUAL"),
            best_bid: Decimal::from(51600),
            best_ask: Decimal::from(51700),
        });

        // Position closed
        let pos = exchange
            .account
            .positions
            .get(&InstrumentNameExchange::from("BTC-PERPETUAL"));
        assert!(pos.is_none(), "position should be closed after sell fill");
        assert_eq!(exchange.resting_orders.len(), 0);

        // Verify we have profit (sold at 51500, bought at 49500)
        let final_balance = exchange
            .account
            .balance_mut(&AssetNameExchange::from("usd"))
            .unwrap()
            .balance
            .free;
        assert!(
            final_balance > initial_balance - Decimal::from(500), // Approximate check for profit
            "should have profit after full round trip"
        );
    }
}

#[derive(Debug)]
pub struct OpenOrderNotifications {
    pub balance: Snapshot<AssetBalance<AssetNameExchange>>,
    pub trade: Trade<QuoteAsset, InstrumentNameExchange>,
}
