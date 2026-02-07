use crate::Identifier;
use barter_integration::{
    error::SocketError,
    protocol::{
        StreamParser,
        websocket::{WebSocket, WsMessage},
    },
    subscription::SubscriptionId,
};
use serde::{Deserialize, Serialize};

/// [`Deribit`](super::Deribit) market data WebSocket message wrapper.
///
/// Deribit wraps all subscription messages in a JSON-RPC 2.0 format with a `params`
/// field containing the `channel` and `data`.
///
/// This wrapper handles both subscription messages (which have `params`) and
/// RPC response messages (which have `result`), gracefully ignoring non-subscription
/// messages by returning a special error variant.
///
/// ### Raw Payload Examples
///
/// #### Subscription Message (has `params`)
/// ```json
/// {
///   "jsonrpc": "2.0",
///   "method": "subscription",
///   "params": {
///     "channel": "trades.BTC-PERPETUAL.raw",
///     "data": [...]
///   }
/// }
/// ```
///
/// #### RPC Response (has `result`, not `params`)
/// ```json
/// {
///   "jsonrpc": "2.0",
///   "id": 9999,
///   "result": {"version": "1.2.26"}
/// }
/// ```
#[derive(Clone, PartialEq, Debug, Serialize)]
pub struct DeribitMessage<T> {
    pub subscription_id: SubscriptionId,
    pub data: T,
}

impl<'de, T> Deserialize<'de> for DeribitMessage<T>
where
    T: Deserialize<'de>,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::de::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Params<T> {
            channel: String,
            data: T,
        }

        #[derive(Deserialize)]
        struct Wrapper<T> {
            params: Params<T>,
        }

        let wrapper = Wrapper::deserialize(deserializer)?;
        let subscription_id =
            parse_deribit_channel(&wrapper.params.channel).map_err(serde::de::Error::custom)?;
        Ok(DeribitMessage {
            subscription_id,
            data: wrapper.params.data,
        })
    }
}

/// Parse a Deribit channel string (e.g., "trades.BTC-PERPETUAL.100ms") into a
/// standard Barter subscription ID format (e.g., "trades|BTC-PERPETUAL").
pub fn parse_deribit_channel(channel: &str) -> Result<SubscriptionId, String> {
    // Deribit channel format: {channel}.{market}.{interval}
    // Example: "trades.BTC-PERPETUAL.100ms"
    let (base, rest) = channel
        .split_once('.')
        .ok_or_else(|| format!("Invalid Deribit channel format: {}", channel))?;

    let market = rest
        .split_once('.')
        .map(|(market, _interval)| market)
        .unwrap_or(rest);

    Ok(SubscriptionId::from(format!("{}|{}", base, market)))
}

impl<T> Identifier<Option<SubscriptionId>> for DeribitMessage<T> {
    fn id(&self) -> Option<SubscriptionId> {
        Some(self.subscription_id.clone())
    }
}

/// Custom [`StreamParser`] for Deribit that filters out RPC response messages.
///
/// Deribit sends heartbeat ping responses that have a `result` field instead of `params`.
/// This parser detects those messages and returns `None` (safe-to-skip) instead of
/// propagating a deserialization error.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Deserialize, Serialize)]
pub struct DeribitParser;

impl<Output> StreamParser<Output> for DeribitParser
where
    Output: for<'de> Deserialize<'de>,
{
    type Stream = WebSocket;
    type Message = WsMessage;
    type Error = barter_integration::protocol::websocket::WsError;

    fn parse(input: Result<Self::Message, Self::Error>) -> Option<Result<Output, SocketError>> {
        use barter_integration::protocol::websocket::{
            process_binary, process_close_frame, process_frame, process_ping, process_pong,
            process_text,
        };

        match input {
            Ok(WsMessage::Text(text)) => {
                // Check if this is an RPC response (has "result" field) rather than
                // a subscription message (has "params" field)
                if is_rpc_response(text.as_str()) {
                    // Silently ignore RPC responses (like ping replies)
                    return None;
                }
                process_text(text)
            }
            Ok(ws_message) => match ws_message {
                WsMessage::Binary(binary) => process_binary(binary),
                WsMessage::Ping(ping) => process_ping(ping),
                WsMessage::Pong(pong) => process_pong(pong),
                WsMessage::Close(close_frame) => process_close_frame(close_frame),
                WsMessage::Frame(frame) => process_frame(frame),
                WsMessage::Text(_) => unreachable!(),
            },
            Err(ws_err) => Some(Err(SocketError::WebSocket(Box::new(ws_err)))),
        }
    }
}

/// Check if a JSON payload is an RPC response (has `result` field without `params`).
///
/// This is a lightweight check that avoids full deserialization.
fn is_rpc_response(text: &str) -> bool {
    // Quick check: RPC responses have "result" but not "params"
    // We do a simple string search for efficiency

    // Must have "result" field
    if !text.contains("\"result\"") {
        return false;
    }

    // Must NOT have "params" field (subscription messages have params)
    !text.contains("\"params\"")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_rpc_response_ping_reply() {
        let ping_reply = r#"{"jsonrpc":"2.0","id":9999,"result":{"version":"1.2.26"}}"#;
        assert!(is_rpc_response(ping_reply));
    }

    #[test]
    fn test_is_rpc_response_subscription() {
        let subscription = r#"{"jsonrpc":"2.0","method":"subscription","params":{"channel":"trades.BTC-PERPETUAL.raw","data":[{"value":123}]}}"#;
        assert!(!is_rpc_response(subscription));
    }

    #[test]
    fn test_is_rpc_response_subscription_response() {
        let sub_response = r#"{"jsonrpc":"2.0","id":1,"result":["trades.BTC-PERPETUAL.raw"]}"#;
        assert!(is_rpc_response(sub_response));
    }

    #[test]
    fn test_parse_channel_valid() {
        let id = parse_deribit_channel("trades.BTC-PERPETUAL.100ms").unwrap();
        assert_eq!(id, SubscriptionId::from("trades|BTC-PERPETUAL"));
    }

    #[test]
    fn test_parse_channel_invalid() {
        let result = parse_deribit_channel("invalid");
        assert!(result.is_err());
    }
}
