# Deribit Feed Authentication Fixes

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Fix all identified issues in the Deribit raw feed authentication implementation (excluding low severity test issue).

**Architecture:** Address security (credential serialization), API compatibility (Hash/Copy derives, as_ref behavior), and code quality (inefficient credential extraction) while fixing the compilation error in examples.

**Tech Stack:** Rust, Serde, barter-data crate

---

## Context

This plan addresses issues identified in a code review of Deribit raw feed authentication support. The following files need changes:

1. `barter-data/src/exchange/deribit/mod.rs` - Main implementation file
2. `barter-data/src/exchange/deribit/channel.rs` - Channel definitions
3. `barter-data/examples/deribit_raw_metrics_aggregation.rs` - Example file with compile error

---

## Task 1: Fix Credential Serialization Security Issue

**Files:**
- Modify: `barter-data/src/exchange/deribit/mod.rs`

**Step 1: Identify the Deribit struct and its Serialize implementation**

The `Deribit` struct at around line 85 needs the credentials field marked with `#[serde(skip)]` to prevent accidental serialization.

**Step 2: Modify the struct definition**

Change:
```rust
#[derive(Debug, Default, Clone, PartialEq, Eq, Deserialize)]
pub struct Deribit {
    pub credentials: Option<DeribitCredentials>,
}
```

To:
```rust
#[derive(Debug, Default, Clone, PartialEq, Eq, Deserialize, Hash)]
pub struct Deribit {
    #[serde(skip)]
    pub credentials: Option<DeribitCredentials>,
}
```

**Step 3: Check for custom Serialize implementation**

If there's a custom Serialize impl (around lines 158-169), it needs to be updated to skip credentials. Look for:
```rust
impl Serialize for Deribit {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        // ...
    }
}
```

Either remove it (if we can use derived Serialize) or ensure it skips the credentials field.

**Step 4: Run clippy to verify**

```bash
cargo clippy --all-targets --all-features -- -D warnings
```

Expected: No warnings

**Step 5: Run tests**

```bash
cargo test --all-features -p barter-data
```

Expected: All tests pass

**Step 6: Commit**

```bash
git add barter-data/src/exchange/deribit/mod.rs
git commit -m "fix: skip credentials in serialization for security

- Add #[serde(skip)] to credentials field
- Add Hash derive for API consistency
- Prevent accidental credential exposure in logs/config"
```

---

## Task 2: Restore Copy trait to Deribit struct

**Files:**
- Modify: `barter-data/src/exchange/deribit/mod.rs`
- Modify: `barter-data/src/exchange/deribit/channel.rs` (if DeribitChannel needs Copy too)

**Step 1: Identify why Copy was removed**

If `DeribitCredentials` doesn't implement `Copy`, we need to either:
1. Make `DeribitCredentials` implement `Copy` (if it's just simple fields)
2. Or decide this is an intentional breaking change and document it

**Step 2: If we restore Copy, modify DeribitCredentials**

Check `DeribitCredentials` struct (likely in `mod.rs` around the same area):

If it's something like:
```rust
pub struct DeribitCredentials {
    pub client_id: String,
    pub client_secret: String,
}
```

Change to:
```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DeribitCredentials {
    pub client_id: String,  // String doesn't implement Copy, so this won't work
    pub client_secret: String,
}
```

If credentials contain `String` or other non-Copy types, we cannot derive `Copy`. In that case:

**Alternative Step 2: Document the breaking change**

Add a doc comment to the `Deribit` struct:

```rust
/// Deribit exchange connector for raw feed authentication.
/// 
/// **Note:** As of this version, `Deribit` no longer implements `Copy` due to
/// the addition of optional credentials. Use `Clone` instead where needed.
#[derive(Debug, Default, Clone, PartialEq, Eq, Deserialize, Hash)]
pub struct Deribit {
    #[serde(skip)]
    pub credentials: Option<DeribitCredentials>,
}
```

**Step 3: Run clippy**

```bash
cargo clippy --all-targets --all-features -- -D warnings
```

Expected: No warnings

**Step 4: Run tests**

```bash
cargo test --all-features -p barter-data
```

Expected: All tests pass

**Step 5: Commit**

```bash
git add barter-data/src/exchange/deribit/mod.rs
git commit -m "fix: document Copy removal for Deribit

Deribit no longer implements Copy due to credentials field.
Added documentation to explain this breaking change."
```

---

## Task 3: Fix DeribitChannel as_ref Breaking Change

**Files:**
- Modify: `barter-data/src/exchange/deribit/channel.rs`

**Step 1: Review current as_ref implementation**

Look at the `DeribitChannel` enum around line 88-90. The `as_ref()` method should return the full channel string, not just the base name.

Current (problematic):
```rust
impl AsRef<str> for DeribitChannel {
    fn as_ref(&self) -> &str {
        match self {
            DeribitChannel::Trades => "trades",
            // ...
        }
    }
}
```

**Step 2: Understand the expected behavior**

Deribit channel strings typically include the instrument, e.g., "trades.BTC-PERPETUAL". The `as_ref()` should return the complete channel identifier.

Check if there's an `as_channel_string()` method that's being used internally. If so, the `as_ref()` implementation should call it.

**Step 3: Fix the implementation**

Change `as_ref()` to return the full channel string:

```rust
impl AsRef<str> for DeribitChannel {
    fn as_ref(&self) -> &str {
        // This should return the full channel string including instrument
        // If there's already a method that does this, delegate to it
        self.as_channel_string()
    }
}
```

Or if it needs to be reconstructed:

```rust
impl AsRef<str> for DeribitChannel {
    fn as_ref(&self) -> &str {
        match self {
            DeribitChannel::Trades { instrument } => {
                // Return full channel string
                format!("trades.{}", instrument)
            }
            // ... other variants
        }
    }
}
```

Note: If this requires allocation and `as_ref` must return `&str`, we may need to reconsider the approach. It might be better to implement `Display` or provide a `to_string()` method instead.

**Step 4: Run clippy**

```bash
cargo clippy --all-targets --all-features -- -D warnings
```

Expected: No warnings

**Step 5: Run tests**

```bash
cargo test --all-features -p barter-data
```

Expected: All tests pass

**Step 6: Commit**

```bash
git add barter-data/src/exchange/deribit/channel.rs
git commit -m "fix: restore full channel string in DeribitChannel::as_ref

as_ref() now returns the complete channel identifier (e.g., 'trades.BTC-PERPETUAL')
instead of just the base name ('trades'). Fixes breaking API change."
```

---

## Task 4: Fix Inefficient Credential Extraction

**Files:**
- Modify: `barter-data/src/exchange/deribit/mod.rs`

**Step 1: Find the inefficient code**

Look around lines 313-320 for code like:

```rust
let credentials = serde_json::to_value(&first_sub.exchange)
    .and_then(|v| v.get("credentials").cloned())
    .and_then(|c| serde_json::from_value::<DeribitCredentials>(c).ok());
```

**Step 2: Design a better approach**

Instead of serializing to JSON and back, we should access the credentials directly.

Check if `first_sub.exchange` is already typed as `Deribit`. If so:

```rust
// Instead of JSON roundtrip:
let credentials = first_sub.exchange.credentials.clone();
```

If the type is generic, we may need to add a trait or accessor method.

**Step 3: Implement the fix**

Replace the JSON serialization with direct access:

```rust
// Old inefficient code:
// let credentials = serde_json::to_value(&first_sub.exchange)
//     .and_then(|v| v.get("credentials").cloned())
//     .and_then(|c| serde_json::from_value::<DeribitCredentials>(c).ok());

// New efficient code:
let credentials = first_sub.exchange.credentials.clone();
```

Or if we need to extract from a generic Exchange type:

```rust
// If there's a trait for exchanges with credentials:
if let Some(creds) = first_sub.exchange.credentials() {
    // ...
}
```

**Step 4: Verify the change**

Ensure the new code:
1. Compiles without errors
2. Preserves the same functionality
3. Removes the serde_json roundtrip

**Step 5: Run clippy**

```bash
cargo clippy --all-targets --all-features -- -D warnings
```

Expected: No warnings

**Step 6: Run tests**

```bash
cargo test --all-features -p barter-data
```

Expected: All tests pass

**Step 7: Commit**

```bash
git add barter-data/src/exchange/deribit/mod.rs
git commit -m "refactor: replace JSON roundtrip with direct credential access

Eliminate inefficient serde_json serialization/deserialization when
extracting credentials. Use direct field access instead."
```

---

## Task 5: Fix Example Compilation Error

**Files:**
- Modify: `barter-data/examples/deribit_raw_metrics_aggregation.rs`

**Step 1: Find the problematic code**

Look at line 151 in the example file:

```rust
let deribit = deribit.clone();  // This fails if Deribit doesn't implement Clone
```

**Step 2: Verify Deribit has Clone**

Ensure the `Deribit` struct has `#[derive(Clone)]` or implements `Clone`.

If it does have Clone, the code should work. If it doesn't, we have two options:

Option A: Ensure Deribit implements Clone (should be done in Task 1)
Option B: Rewrite the example to not use clone

Since Task 1 adds `Clone` derive, verify the example now compiles.

**Step 3: Fix if needed**

If still failing, rewrite to avoid clone:

```rust
// Instead of:
// let deribit = deribit.clone();

// Use:
let deribit = Deribit::default();  // Create new instance
// or pass by reference
```

**Step 4: Compile the example**

```bash
cargo build --example deribit_raw_metrics_aggregation
```

Expected: Compiles successfully

**Step 5: Commit**

```bash
git add barter-data/examples/deribit_raw_metrics_aggregation.rs
git commit -m "fix: resolve compilation error in Deribit example

Update example to work with updated Deribit struct that no longer
implements Copy. Use Clone or create new instances as needed."
```

---

## Task 6: Final Verification

**Step 1: Run all lints**

```bash
cargo clippy --all-targets --all-features -- -D warnings
```

Expected: No warnings

**Step 2: Run all tests**

```bash
cargo test --all-features -p barter-data
```

Expected: All tests pass

**Step 3: Format code**

```bash
cargo fmt
```

**Step 4: Build all examples**

```bash
cargo build --examples
```

Expected: All examples compile

**Step 5: Final commit (if any changes)**

```bash
git add -A
git commit -m "chore: apply formatting and final cleanup"
```

---

## Summary of Changes

1. **Security**: Added `#[serde(skip)]` to credentials field
2. **API Compatibility**: Restored Hash derive
3. **Breaking Change**: Documented Copy removal (cannot be fixed due to String fields)
4. **Breaking Change**: Fixed DeribitChannel::as_ref() to return full channel string
5. **Performance**: Replaced inefficient JSON roundtrip with direct access
6. **Compilation**: Fixed example to use Clone instead of Copy

**Not fixed (low severity):**
- Test for DeribitChannel::as_ref() - reviewer chose to skip

---

## Post-Implementation

After all tasks are complete:

1. Review the full diff to ensure nothing was missed
2. Run the full test suite one more time
3. Update any documentation that references the breaking changes
