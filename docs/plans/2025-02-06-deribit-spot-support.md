# Deribit Spot Support Implementation Plan

**Goal:** Add real Spot instrument support to Deribit instead of mapping Spot to perpetuals.

**Architecture:** Deribit uses the same WebSocket API for all instrument types (spot, perpetual, future, option). Spot markets use `{BASE}_{QUOTE}` format (e.g., `BTC_USDC`), while perpetuals use `{BASE}-PERPETUAL`. We need to update the market naming logic and subscription validation.

**Tech Stack:** Rust, barter-data library

---

## Task 1: Add Spot Support in Deribit Market Naming

**Files:**
- Modify: `barter-data/src/exchange/deribit/market.rs:32-58`

**Step 1: Modify Spot branch in deribit_market function**

Current code:
```rust
Spot => {
    // Deribit doesn't have spot markets, but we can map to perpetual
    format_smolstr!("{base}-PERPETUAL").to_uppercase_smolstr()
}
```

New code:
```rust
Spot => {
    // Deribit spot markets use format: BTC_USDC, ETH_USDT, ETH_BTC
    format_smolstr!("{base}_{quote}").to_uppercase_smolstr()
}
```

**Step 2: Run existing tests**

Run: `cargo test --package barter-data deribit_market`

Expected: Tests pass (we'll update Spot test in Task 3)

**Step 3: Commit**

```bash
git add barter-data/src/exchange/deribit/market.rs
git commit -m "fix(deribit): add Spot market naming support"
```

---

## Task 2: Update Subscription Validation for Deribit Spot

**Files:**
- Modify: `barter-data/src/subscription/mod.rs:280-284`

**Step 1: Add Spot to Deribit's supported kinds**

Current code (around line 280):
```rust
(
    Deribit,
    Perpetual | Future { .. } | Option { .. },
    PublicTrades | OrderBooksL1 | OrderBooksL2,
) => true,
```

New code:
```rust
(
    Deribit,
    Spot | Perpetual | Future { .. } | Option { .. },
    PublicTrades | OrderBooksL1 | OrderBooksL2,
) => true,
```

**Step 2: Also update exchange_supports_instrument_kind (around line 205)**

Add Deribit to Spot support:
```rust
(Deribit, Spot) => true,
```

**Step 3: Run tests**

Run: `cargo test --package barter-data subscription`

Expected: Tests pass

**Step 4: Commit**

```bash
git add barter-data/src/subscription/mod.rs
git commit -m "fix(subscription): enable Deribit Spot subscriptions"
```

---

## Task 3: Add Comprehensive Tests for Deribit Spot

**Files:**
- Modify: `barter-data/src/exchange/deribit/market.rs:70-147`

**Step 1: Add Spot market tests**

Add new test function after the existing tests:

```rust
#[test]
fn test_deribit_market_spot_btc_usdc() {
    let instrument = test_instrument("btc", "usdc", Spot);
    let market = deribit_market(&instrument);
    assert_eq!(market.as_ref(), "BTC_USDC");
}

#[test]
fn test_deribit_market_spot_eth_btc() {
    let instrument = test_instrument("eth", "btc", Spot);
    let market = deribit_market(&instrument);
    assert_eq!(market.as_ref(), "ETH_BTC");
}

#[test]
fn test_deribit_market_spot_eth_usdt() {
    let instrument = test_instrument("eth", "usdt", Spot);
    let market = deribit_market(&instrument);
    assert_eq!(market.as_ref(), "ETH_USDT");
}
```

**Step 2: Update existing Spot fallback test**

Replace the existing `test_deribit_market_spot_fallback` test with:

```rust
#[test]
fn test_deribit_market_spot() {
    // Spot markets should use {BASE}_{QUOTE} format
    let instrument = test_instrument("btc", "usdc", Spot);
    let market = deribit_market(&instrument);
    assert_eq!(market.as_ref(), "BTC_USDC");
}
```

**Step 3: Run tests**

Run: `cargo test --package barter-data deribit_market`

Expected: All 6 tests pass

**Step 4: Commit**

```bash
git add barter-data/src/exchange/deribit/market.rs
git commit -m "test(deribit): add Spot market naming tests"
```

---

## Task 4: Verify Implementation

**Files:**
- All modified files

**Step 1: Run all barter-data tests**

Run: `cargo test --package barter-data`

Expected: All tests pass

**Step 2: Run clippy**

Run: `cargo clippy --package barter-data --all-targets --all-features -- -D warnings`

Expected: No warnings

**Step 3: Format code**

Run: `cargo fmt --package barter-data`

Expected: Code formatted

**Step 4: Commit final changes**

```bash
git commit -m "refactor(deribit): final Spot support formatting" || echo "No changes to commit"
```

---

## Summary

After completion:
1. Deribit Spot markets will use correct naming (`BTC_USDC` instead of `BTC-PERPETUAL`)
2. Deribit will accept Spot subscriptions for trades, L1, and L2
3. Existing tests updated and new tests added
4. All lints and tests passing

## Manual Testing (Optional)

To verify with real Deribit API:

```rust
use barter_data::exchange::deribit::Deribit;
use barter_data::subscription::Subscription;
use barter_data::subscription::trade::PublicTrades;
use barter_instrument::instrument::market_data::MarketDataInstrument;
use barter_instrument::instrument::market_data::kind::MarketDataInstrumentKind;

let instrument = MarketDataInstrument::from(("btc", "usdc", MarketDataInstrumentKind::Spot));
let sub = Subscription::<Deribit, _, PublicTrades>::new(Deribit, instrument, PublicTrades);
// Should validate successfully
```
