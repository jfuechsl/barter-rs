# Barter-Data Migration Guide

## Table of Contents

1. [Migrating to OrderBookL2Sequencer Pattern](#migrating-to-orderbookl2sequencer-pattern)
2. [Adding a New Exchange Connector](#adding-a-new-exchange-connector)
3. [API Stability Notes](#api-stability-notes)

---

## Migrating to OrderBookL2Sequencer Pattern

### Overview

The sequencer pattern separates validation logic from transformation, improving testability.

### Before (Inline Validation)

```rust
impl Transformer for MyExchangeL2Transformer<InstrumentKey> {
    fn transform(&mut self, input: Self::Input) -> Self::OutputIter {
        // Validation mixed with transformation
        if input.sequence <= self.last_sequence {
            return vec![];
        }
        // ...
    }
}
```

### After (Sequencer Pattern)

```rust
// 1. Define sequencer
pub struct MyExchangeOrderBookL2Sequencer {
    pub last_update_id: u64,
}

impl MyExchangeOrderBookL2Sequencer {
    pub fn validate_sequence(&mut self, update: Update) -> Result<Option<Update>, DataError> {
        if update.sequence <= self.last_update_id {
            return Ok(None);
        }
        if update.sequence != self.last_update_id + 1 {
            return Err(DataError::InvalidSequence { /* ... */ });
        }
        self.last_update_id = update.sequence;
        Ok(Some(update))
    }
}

// 2. Use in transformer
impl Transformer for MyExchangeL2Transformer<InstrumentKey> {
    fn transform(&mut self, input: Self::Input) -> Self::OutputIter {
        let instrument = self.instrument_map.find_mut(&input.id())?;
        match instrument.sequencer.validate_sequence(input) {
            Ok(Some(update)) => /* transform */,
            Ok(None) => vec![],
            Err(e) => vec![Err(e)],
        }
    }
}
```

---

## Adding a New Exchange Connector

### Directory Structure

```
barter-data/src/exchange/my_exchange/
├── mod.rs          # Connector struct, ExchangeId mapping
├── market.rs       # Market identifier
├── channel.rs      # WebSocket channels
└── book/l2.rs      # L2 transformer and sequencer
```

### Steps

1. Implement `Connector` trait in `mod.rs`
2. Implement `StreamSelector` for each subscription kind
3. Add match arm in `DynamicStreams::init` if needed
4. Add tests following patterns in `binance/spot/l2.rs`

---

## API Stability Notes

### Stable Public APIs
- `Streams` builder
- `DynamicStreams::init()`
- `MarketEvent<InstrumentKey, Kind::Event>`
- All `SubscriptionKind` types

### Internal/Unstable
- Sequencer trait definitions
- Transformer type parameters
- Channel/Txs/Rxs structures
