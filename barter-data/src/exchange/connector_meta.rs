/// Metadata for stream connector types.
///
/// Enables `define_stream_connectors!` to discover connector metadata
/// without hardcoding it in the macro.
///
/// # Purpose
///
/// This trait provides compile-time metadata about exchange connectors,
/// allowing the `define_stream_connectors!` macro to generate correct
/// module paths and type names without maintaining a hardcoded mapping.
///
/// # Implementation
///
/// Typically implemented via the `#[derive(StreamConnectorMeta)]` macro:
///
/// ```rust,ignore
/// #[derive(StreamConnectorMeta)]
/// #[connector(exchange = "coinbase")]
/// pub struct Coinbase;
/// ```
///
/// For generic connectors, implement on the type parameter:
///
/// ```rust,ignore
/// #[derive(StreamConnectorMeta)]
/// #[connector(exchange = "binance", sub_module = "spot")]
/// pub struct BinanceServerSpot;
///
/// pub type BinanceSpot = Binance<BinanceServerSpot>;
/// ```
pub trait StreamConnectorMeta {
    /// Exchange module root (e.g., "binance", "kraken").
    ///
    /// This corresponds to the directory name under `barter-data/src/exchange/`.
    const EXCHANGE_ROOT: &'static str;

    /// Sub-module within exchange (e.g., Some("spot"), Some("futures"), None).
    ///
    /// For exchanges with multiple connector types (like Binance with spot and futures),
    /// this specifies the subdirectory. For simple exchanges, this is `None`.
    const SUB_MODULE: Option<&'static str>;

    /// Market identifier type name (e.g., "BinanceMarket", "KrakenMarket").
    ///
    /// This is the name of the type that implements the market identification
    /// for this exchange.
    const MARKET_TYPE: &'static str;

    /// Channel identifier type name (e.g., "BinanceChannel", "KrakenChannel").
    ///
    /// This is the name of the type that represents subscription channels
    /// for this exchange.
    const CHANNEL_TYPE: &'static str;
}
