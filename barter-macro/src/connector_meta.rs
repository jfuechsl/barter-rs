use convert_case::{Case, Casing};
use darling::FromDeriveInput;
use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, DeriveInput};

/// Options parsed from `#[derive(StreamConnectorMeta)]` and `#[connector(...)]` attributes.
#[derive(FromDeriveInput)]
#[darling(attributes(connector))]
struct ConnectorOpts {
    /// The name of the type being derived on
    ident: syn::Ident,

    /// Exchange module root (required)
    ///
    /// Example: "binance", "kraken", "coinbase"
    exchange: String,

    /// Sub-module path (optional)
    ///
    /// Example: Some("spot"), Some("futures"), None
    #[darling(default)]
    sub_module: Option<String>,

    /// Market type name (optional, defaults to {Exchange}Market)
    ///
    /// Example: "BinanceMarket", "KrakenMarket"
    #[darling(default)]
    market: Option<String>,

    /// Channel type name (optional, defaults to {Exchange}Channel)
    ///
    /// Example: "BinanceChannel", "KrakenChannel"
    #[darling(default)]
    channel: Option<String>,
}

/// Derives the `StreamConnectorMeta` trait for a connector type.
///
/// This derive macro generates an implementation of the `StreamConnectorMeta` trait
/// based on attributes provided in `#[connector(...)]`.
///
/// # Required Attributes
///
/// - `exchange`: The exchange module root (e.g., "binance", "kraken")
///
/// # Optional Attributes
///
/// - `sub_module`: Sub-module within the exchange (e.g., "spot", "futures")
/// - `market`: Market type name (defaults to `{Exchange}Market` in PascalCase)
/// - `channel`: Channel type name (defaults to `{Exchange}Channel` in PascalCase)
///
/// # Examples
///
/// Simple connector:
/// ```rust,ignore
/// #[derive(StreamConnectorMeta)]
/// #[connector(exchange = "coinbase")]
/// pub struct Coinbase;
/// ```
///
/// Connector with sub-module:
/// ```rust,ignore
/// #[derive(StreamConnectorMeta)]
/// #[connector(exchange = "binance", sub_module = "spot")]
/// pub struct BinanceServerSpot;
/// ```
///
/// Connector with custom type names:
/// ```rust,ignore
/// #[derive(StreamConnectorMeta)]
/// #[connector(
///     exchange = "custom",
///     market = "CustomMarketType",
///     channel = "CustomChannelType"
/// )]
/// pub struct CustomConnector;
/// ```
pub fn derive_stream_connector_meta(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    let opts = match ConnectorOpts::from_derive_input(&input) {
        Ok(opts) => opts,
        Err(e) => return e.write_errors().into(),
    };

    let name = &opts.ident;
    let exchange_root = &opts.exchange;

    // Convert exchange to PascalCase for type names
    let exchange_pascal = exchange_root.to_case(Case::Pascal);

    let sub_module = match &opts.sub_module {
        Some(s) => quote! { Some(#s) },
        None => quote! { None },
    };

    let market_type = opts
        .market
        .unwrap_or_else(|| format!("{}Market", exchange_pascal));

    let channel_type = opts
        .channel
        .unwrap_or_else(|| format!("{}Channel", exchange_pascal));

    let expanded = quote! {
        impl crate::exchange::connector_meta::StreamConnectorMeta for #name {
            const EXCHANGE_ROOT: &'static str = #exchange_root;
            const SUB_MODULE: Option<&'static str> = #sub_module;
            const MARKET_TYPE: &'static str = #market_type;
            const CHANNEL_TYPE: &'static str = #channel_type;
        }
    };

    expanded.into()
}
