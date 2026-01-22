# Developer Quick Start Guide

This is a quick reference for developers adding new exchanges or subscription kinds to Barter-Data. For comprehensive documentation, see the [developer guide](https://docs.rs/barter-data/latest/barter_data/developer_guide/index.html) in the API documentation.

## Adding a New Exchange: Quick Checklist

### 1. Module Structure
```
src/exchange/my_exchange/
├── mod.rs           # Connector impl
├── channel.rs       # Channel type
├── market.rs        # Market type
├── subscription.rs  # SubResponse type
└── trade.rs         # PublicTrades impl (example)
```

### 2. Implement Required Traits

```rust
// mod.rs - Connector
impl Connector for MyExchange {
    const ID: ExchangeId = ExchangeId::MyExchange;
    type Channel = MyExchangeChannel;
    type Market = MyExchangeMarket;
    type Subscriber = WebSocketSubscriber;
    type SubValidator = WebSocketSubValidator;
    type SubResponse = MyExchangeSubResponse;

    fn url() -> Result<Url, SocketError> { /* ... */ }
    fn requests(subs: Vec<ExchangeSub<...>>) -> Vec<WsMessage> { /* ... */ }
}

// channel.rs - Map subscriptions to channels
impl<I> Identifier<MyExchangeChannel> for Subscription<MyExchange, I, PublicTrades> {
    fn id(&self) -> MyExchangeChannel { MyExchangeChannel::TRADES }
}

// market.rs - Map instruments to market identifiers
impl<I: InstrumentData> Identifier<MyExchangeMarket> for Subscription<MyExchange, I, PublicTrades> {
    fn id(&self) -> MyExchangeMarket {
        MyExchangeMarket(format!("{}{}", self.instrument.base_asset(), self.instrument.quote_asset()))
    }
}

// subscription.rs - Validate subscription responses
impl Validator for MyExchangeSubResponse {
    fn validate(self) -> Result<Self, SocketError> { /* ... */ }
}

// trade.rs - StreamSelector + message types
impl<I: InstrumentData> StreamSelector<I, PublicTrades> for MyExchange {
    type SnapFetcher = NoInitialSnapshots;
    type Stream = ExchangeWsStream<WebSocketSerdeParser, StatelessTransformer<...>>;
}

impl<K: Clone> From<(ExchangeId, K, MyExchangeTrades)> for MarketIter<K, PublicTrade> {
    fn from(...) -> Self { /* transform to Barter events */ }
}
```

### 3. Add to Module Exports
Add to `src/exchange/mod.rs`:
```rust
pub mod my_exchange;
```

### 4. Testing
- Unit tests for deserialization
- Integration test/example with live data
- Run `cargo test --all-features`
- Run `cargo clippy --all-targets --all-features`

## Adding a New Subscription Kind: Quick Checklist

### 1. Create Subscription Module
```rust
// src/subscription/my_kind.rs
pub struct MyKind;

impl SubscriptionKind for MyKind {
    type Event = MyKindEvent;
    fn as_str(&self) -> &'static str { "my_kind" }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct MyKindEvent {
    // Your normalized event fields
}
```

### 2. Add to Subscription Module
In `src/subscription/mod.rs`:
```rust
pub mod my_kind;
```

### 3. Implement for Each Exchange
For each supporting exchange:
```rust
// In exchange's module
impl<I: InstrumentData> StreamSelector<I, MyKind> for MyExchange {
    type SnapFetcher = NoInitialSnapshots;
    type Stream = /* ... */;
}

// Create exchange-specific message types and transformers
```

### 4. Optional: Add to SubKind Enum
If this is a "core" subscription kind, add to `SubKind` enum in `src/subscription/mod.rs` and update validation functions.

## Common Patterns

### Stateless Subscriptions (Trades, Tickers)
```rust
type SnapFetcher = NoInitialSnapshots;
type Stream = ExchangeWsStream<WebSocketSerdeParser, StatelessTransformer<...>>;
```

### Stateful Subscriptions (OrderBooks)
```rust
type SnapFetcher = MyExchangeOrderBookL2SnapshotFetcher;
type Stream = ExchangeWsStream<WebSocketSerdeParser, MyExchangeOrderBooksL2Transformer<InstrumentKey>>;
```

### Custom Pings
```rust
fn ping_interval() -> Option<PingInterval> {
    Some(PingInterval {
        interval: tokio::time::interval(Duration::from_secs(30)),
        ping: || WsMessage::text("ping"),
    })
}
```

## Reference Implementations

- **Simple exchange**: `src/exchange/okx/` - PublicTrades only
- **Complex exchange**: `src/exchange/binance/` - Multiple servers, stateful OrderBooks
- **Stateless subscription**: `src/subscription/trade.rs`
- **Stateful subscription**: `src/subscription/book.rs` + Binance L2 transformers

## Resources

- [Full Developer Guide](https://docs.rs/barter-data/latest/barter_data/developer_guide/index.html)
- [API Documentation](https://docs.rs/barter-data)
- [Discord Community](https://discord.gg/wE7RqhnQMV)
- [Examples Directory](../examples/)
