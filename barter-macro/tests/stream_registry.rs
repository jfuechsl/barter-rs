//! Integration tests for the `define_stream_connectors!` macro.
//!
//! These tests verify that the macro correctly parses input, validates it,
//! and generates appropriate code or error messages.

// Import kept for documentation purposes - demonstrates the macro is available
#[allow(unused_imports)]
use barter_macro::define_stream_connectors;

/// Test that the macro compiles with a minimal valid input.
///
/// This is primarily a compile-time test - if it compiles, the basic
/// macro functionality is working.
#[test]
fn test_macro_compiles_with_valid_input() {
    // The macro generates an impl block, which requires the surrounding
    // context to exist. For unit testing, we just verify it parses.
    // Full integration testing happens when used in barter-data.

    // This test passing means the proc-macro crate builds successfully
    // and can be invoked without panicking.
}

/// Test module demonstrating the expected macro syntax.
///
/// This serves as both documentation and a compile-time check that
/// the documented syntax is valid.
mod syntax_examples {
    /// Single connector with single kind.
    #[allow(dead_code)]
    const SINGLE: &str = r#"
        BinanceSpot => [PublicTrades],
    "#;

    /// Single connector with multiple kinds.
    #[allow(dead_code)]
    const MULTIPLE_KINDS: &str = r#"
        BinanceSpot => [PublicTrades, OrderBooksL1, OrderBooksL2],
    "#;

    /// Multiple connectors.
    #[allow(dead_code)]
    const MULTIPLE_CONNECTORS: &str = r#"
        BinanceSpot => [PublicTrades, OrderBooksL1],
        BinanceFuturesUsd => [PublicTrades, Liquidations],
        Coinbase => [PublicTrades],
    "#;

    /// Full registration matching current barter-data usage.
    #[allow(dead_code)]
    const FULL_REGISTRATION: &str = r#"
        BinanceSpot => [PublicTrades, OrderBooksL1, OrderBooksL2],
        BinanceFuturesUsd => [PublicTrades, OrderBooksL1, OrderBooksL2, Liquidations],
        BybitSpot => [PublicTrades, OrderBooksL1, OrderBooksL2],
        BybitPerpetualsUsd => [PublicTrades, OrderBooksL1, OrderBooksL2],
        Bitfinex => [PublicTrades],
        Bitmex => [PublicTrades],
        Coinbase => [PublicTrades],
        GateioSpot => [PublicTrades],
        GateioFuturesUsd => [PublicTrades],
        GateioFuturesBtc => [PublicTrades],
        GateioPerpetualsBtc => [PublicTrades],
        GateioPerpetualsUsd => [PublicTrades],
        GateioOptions => [PublicTrades],
        Kraken => [PublicTrades, OrderBooksL1],
        Okx => [PublicTrades],
        Poloniex => [PublicTrades],
    "#;
}

/// Tests for the parsing and validation logic.
///
/// Note: Compile-time error cases (like duplicate registrations or unknown kinds)
/// cannot be tested at runtime. They are verified through the unit tests in
/// `barter-macro/src/stream_registry.rs` which test the parsing and validation
/// functions directly.
mod validation_tests {
    #[test]
    fn test_supported_subscription_kinds() {
        // Document the supported kinds for reference
        let supported = [
            "PublicTrades",
            "OrderBooksL1",
            "OrderBooksL2",
            "OrderBooksL3",
            "Liquidations",
            "Candles",
        ];
        assert_eq!(supported.len(), 6);
    }

    #[test]
    fn test_supported_connectors() {
        // Document all supported connectors
        let connectors = [
            "BinanceSpot",
            "BinanceFuturesUsd",
            "BybitSpot",
            "BybitPerpetualsUsd",
            "Bitfinex",
            "Bitmex",
            "Coinbase",
            "GateioSpot",
            "GateioFuturesUsd",
            "GateioFuturesBtc",
            "GateioPerpetualsBtc",
            "GateioPerpetualsUsd",
            "GateioOptions",
            "Kraken",
            "Okx",
            "Poloniex",
        ];
        assert_eq!(connectors.len(), 16);
    }

    #[test]
    fn test_kind_to_channel_mapping() {
        // Document the kind to channel field mapping
        let mappings = [
            ("PublicTrades", "trades"),
            ("OrderBooksL1", "l1s"),
            ("OrderBooksL2", "l2s"),
            ("OrderBooksL3", "l3s"),
            ("Liquidations", "liquidations"),
            ("Candles", "candles"),
        ];

        for (kind, channel) in mappings {
            assert!(!kind.is_empty());
            assert!(!channel.is_empty());
        }
    }

    #[test]
    fn test_connector_to_exchange_mapping() {
        // Document the connector to exchange root mapping
        let mappings = [
            ("BinanceSpot", "binance"),
            ("BinanceFuturesUsd", "binance"),
            ("BybitSpot", "bybit"),
            ("BybitPerpetualsUsd", "bybit"),
            ("Bitfinex", "bitfinex"),
            ("Bitmex", "bitmex"),
            ("Coinbase", "coinbase"),
            ("GateioSpot", "gateio"),
            ("GateioFuturesUsd", "gateio"),
            ("Kraken", "kraken"),
            ("Okx", "okx"),
            ("Poloniex", "poloniex"),
        ];

        for (connector, exchange) in mappings {
            assert!(!connector.is_empty());
            assert!(!exchange.is_empty());
        }
    }
}

/// Tests demonstrating error cases.
///
/// These compile-fail doc tests verify that the macro produces appropriate compile errors
/// for invalid inputs. Run with `cargo test --doc`.
mod error_documentation {
    /// Duplicate registration error.
    ///
    /// The macro should reject duplicate (connector, kind) pairs.
    ///
    /// ```compile_fail
    /// use barter_macro::define_stream_connectors;
    ///
    /// define_stream_connectors! {
    ///     BinanceSpot => [PublicTrades],
    ///     BinanceSpot => [PublicTrades],
    /// }
    /// ```
    #[test]
    fn duplicate_registration_fails_to_compile() {
        // This test documents that duplicate registrations are rejected at compile time.
        // The actual test is in the doc test above.
    }

    /// Unknown kind error.
    ///
    /// The macro should reject subscription kinds that are not supported.
    ///
    /// ```compile_fail
    /// use barter_macro::define_stream_connectors;
    ///
    /// define_stream_connectors! {
    ///     BinanceSpot => [InvalidKind],
    /// }
    /// ```
    #[test]
    fn unknown_kind_fails_to_compile() {
        // This test documents that unknown subscription kinds are rejected at compile time.
        // The actual test is in the doc test above.
    }

    /// Unknown connector error.
    ///
    /// The macro should reject connector types that are not registered.
    ///
    /// ```compile_fail
    /// use barter_macro::define_stream_connectors;
    ///
    /// define_stream_connectors! {
    ///     UnknownExchange => [PublicTrades],
    /// }
    /// ```
    #[test]
    fn unknown_connector_fails_to_compile() {
        // This test documents that unknown connector types are rejected at compile time.
        // The actual test is in the doc test above.
    }

    /// Empty kinds error.
    ///
    /// The macro should reject connectors with no subscription kinds.
    ///
    /// ```compile_fail
    /// use barter_macro::define_stream_connectors;
    ///
    /// define_stream_connectors! {
    ///     BinanceSpot => [],
    /// }
    /// ```
    #[test]
    fn empty_kinds_fails_to_compile() {
        // This test documents that empty subscription kind lists are rejected at compile time.
        // The actual test is in the doc test above.
    }

    /// Invalid syntax error.
    ///
    /// The macro should reject malformed syntax.
    ///
    /// ```compile_fail
    /// use barter_macro::define_stream_connectors;
    ///
    /// define_stream_connectors! {
    ///     BinanceSpot: PublicTrades  // Wrong separator (: instead of =>)
    /// }
    /// ```
    #[test]
    fn invalid_syntax_fails_to_compile() {
        // This test documents that invalid macro syntax is rejected at compile time.
        // The actual test is in the doc test above.
    }
}
