use super::{channel::DeribitChannel, market::DeribitMarket};
use crate::exchange::subscription::ExchangeSub;
use barter_integration::{Validator, error::SocketError};
use serde::{Deserialize, Serialize, Serializer, ser::SerializeStruct};

// Implement custom Serialize to assist aesthetics of <Deribit as Connector>::requests() function.
impl Serialize for ExchangeSub<DeribitChannel, DeribitMarket> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("DeribitSubArg", 2)?;
        state.serialize_field("channel", self.channel.as_ref())?;
        state.serialize_field("instrument", self.market.as_ref())?;
        state.end()
    }
}

/// [`Deribit`](super::Deribit) WebSocket subscription response.
///
/// Deribit returns subscription results as an array of subscribed channels on success,
/// or an error object on failure.
///
/// ### Raw Payload Examples
/// #### Subscription Success Response
/// ```json
/// {
///   "jsonrpc": "2.0",
///   "id": 1,
///   "result": ["trades.BTC-PERPETUAL.raw", "ticker.ETH-PERPETUAL.raw"]
/// }
/// ```
///
/// #### Subscription Error Response
/// ```json
/// {
///   "jsonrpc": "2.0",
///   "id": 1,
///   "error": {
///     "code": 10004,
///     "message": "Invalid channel"
///   }
/// }
/// ```
///
/// See docs: <https://docs.deribit.com/api-reference/websocket>
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Deserialize, Serialize)]
pub struct DeribitSubResponse {
    pub jsonrpc: String,
    pub id: u64,
    #[serde(flatten)]
    pub result: DeribitSubResult,
}

#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Deserialize, Serialize)]
#[serde(untagged)]
pub enum DeribitSubResult {
    Success { result: Vec<String> },
    Error { error: DeribitError },
}

#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Deserialize, Serialize)]
pub struct DeribitError {
    pub code: i64,
    pub message: String,
}

impl Validator for DeribitSubResponse {
    fn validate(self) -> Result<Self, SocketError>
    where
        Self: Sized,
    {
        match &self.result {
            DeribitSubResult::Success { .. } => Ok(self),
            DeribitSubResult::Error { error } => Err(SocketError::Subscribe(format!(
                "received failure subscription response code: {} with message: {}",
                error.code, error.message
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod de {
        use super::*;

        #[test]
        fn test_deribit_subscription_success_response() {
            let input = r#"
            {
                "jsonrpc": "2.0",
                "id": 1,
                "result": ["trades.BTC-PERPETUAL.raw", "ticker.ETH-PERPETUAL.raw"]
            }
            "#;

            let actual = serde_json::from_str::<DeribitSubResponse>(input).unwrap();

            match actual.result {
                DeribitSubResult::Success { result } => {
                    assert_eq!(result.len(), 2);
                    assert_eq!(result[0], "trades.BTC-PERPETUAL.raw");
                    assert_eq!(result[1], "ticker.ETH-PERPETUAL.raw");
                }
                DeribitSubResult::Error { .. } => panic!("Expected success, got error"),
            }
        }

        #[test]
        fn test_deribit_subscription_error_response() {
            let input = r#"
            {
                "jsonrpc": "2.0",
                "id": 1,
                "error": {
                    "code": 10004,
                    "message": "Invalid channel"
                }
            }
            "#;

            let actual = serde_json::from_str::<DeribitSubResponse>(input).unwrap();

            match actual.result {
                DeribitSubResult::Success { .. } => panic!("Expected error, got success"),
                DeribitSubResult::Error { error } => {
                    assert_eq!(error.code, 10004);
                    assert_eq!(error.message, "Invalid channel");
                }
            }
        }
    }

    #[test]
    fn test_validate_deribit_sub_response_success() {
        let response = DeribitSubResponse {
            jsonrpc: "2.0".to_string(),
            id: 1,
            result: DeribitSubResult::Success {
                result: vec!["trades.BTC-PERPETUAL.raw".to_string()],
            },
        };

        assert!(response.validate().is_ok());
    }

    #[test]
    fn test_validate_deribit_sub_response_error() {
        let response = DeribitSubResponse {
            jsonrpc: "2.0".to_string(),
            id: 1,
            result: DeribitSubResult::Error {
                error: DeribitError {
                    code: 10004,
                    message: "Invalid channel".to_string(),
                },
            },
        };

        assert!(response.validate().is_err());
    }
}
