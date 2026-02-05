use super::Deribit;
use crate::impl_market_identifier_for_instrument;
use barter_instrument::instrument::{
    kind::option::OptionKind,
    market_data::{MarketDataInstrument, kind::MarketDataInstrumentKind::*},
};
use chrono::DateTime;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use smol_str::{SmolStr, StrExt, format_smolstr};

/// Type that defines how to translate a Barter [`Subscription`] into a
/// [`Deribit`] market that can be subscribed to.
///
/// Deribit instrument naming conventions:
/// - Perpetual: `BTC-PERPETUAL`, `ETH-PERPETUAL`
/// - Futures: `BTC-29SEP23` (expiry: DDMMMYY)
/// - Options: `BTC-27MAY20-9000-C` (strike price + C/P)
///
/// See docs: <https://docs.deribit.com/api-reference/market-data/public-get_instruments>
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Deserialize, Serialize)]
pub struct DeribitMarket(pub SmolStr);

impl_market_identifier_for_instrument!(Deribit => DeribitMarket, deribit_market);

impl AsRef<str> for DeribitMarket {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

fn deribit_market(instrument: &MarketDataInstrument) -> DeribitMarket {
    let MarketDataInstrument {
        base,
        quote: _,
        kind,
    } = instrument;

    DeribitMarket(match kind {
        Spot => {
            // Deribit doesn't have spot markets, but we can map to perpetual
            format_smolstr!("{base}-PERPETUAL").to_uppercase_smolstr()
        }
        Future(contract) => {
            format_smolstr!("{base}-{}", format_expiry(contract.expiry)).to_uppercase_smolstr()
        }
        Perpetual => format_smolstr!("{base}-PERPETUAL").to_uppercase_smolstr(),
        Option(contract) => format_smolstr!(
            "{base}-{}-{}-{}",
            format_expiry(contract.expiry),
            contract.strike,
            match contract.kind {
                OptionKind::Call => "C",
                OptionKind::Put => "P",
            },
        )
        .to_uppercase_smolstr(),
    })
}

/// Format the expiry DateTime<Utc> to be Deribit API compatible.
///
/// Deribit uses format: `27MAY24` (day + month abbreviation + 2-digit year)
///
/// See docs: <https://docs.deribit.com/api-reference/market-data/public-get_instruments>
fn format_expiry(expiry: DateTime<Utc>) -> String {
    expiry.format("%d%b%y").to_string().to_uppercase()
}

#[cfg(test)]
mod tests {
    use super::*;
    use barter_instrument::instrument::market_data::kind::MarketDataInstrumentKind;
    use chrono::TimeZone;

    fn test_instrument(
        base: &str,
        quote: &str,
        kind: MarketDataInstrumentKind,
    ) -> MarketDataInstrument {
        MarketDataInstrument {
            base: base.into(),
            quote: quote.into(),
            kind,
        }
    }

    #[test]
    fn test_deribit_market_perpetual() {
        let instrument = test_instrument("btc", "usd", Perpetual);
        let market = deribit_market(&instrument);
        assert_eq!(market.as_ref(), "BTC-PERPETUAL");
    }

    #[test]
    fn test_deribit_market_future() {
        use barter_instrument::instrument::market_data::kind::MarketDataFutureContract;
        let expiry = Utc.with_ymd_and_hms(2024, 9, 27, 8, 0, 0).unwrap();
        let instrument = test_instrument("btc", "usd", Future(MarketDataFutureContract { expiry }));
        let market = deribit_market(&instrument);
        assert_eq!(market.as_ref(), "BTC-27SEP24");
    }

    #[test]
    fn test_deribit_market_option_call() {
        use barter_instrument::instrument::market_data::kind::MarketDataOptionContract;
        let expiry = Utc.with_ymd_and_hms(2024, 5, 27, 8, 0, 0).unwrap();
        let instrument = test_instrument(
            "btc",
            "usd",
            Option(MarketDataOptionContract {
                expiry,
                strike: rust_decimal::Decimal::from(9000),
                kind: OptionKind::Call,
                exercise: barter_instrument::instrument::kind::option::OptionExercise::European,
            }),
        );
        let market = deribit_market(&instrument);
        assert_eq!(market.as_ref(), "BTC-27MAY24-9000-C");
    }

    #[test]
    fn test_deribit_market_option_put() {
        use barter_instrument::instrument::market_data::kind::MarketDataOptionContract;
        let expiry = Utc.with_ymd_and_hms(2024, 5, 27, 8, 0, 0).unwrap();
        let instrument = test_instrument(
            "btc",
            "usd",
            Option(MarketDataOptionContract {
                expiry,
                strike: rust_decimal::Decimal::from(9000),
                kind: OptionKind::Put,
                exercise: barter_instrument::instrument::kind::option::OptionExercise::European,
            }),
        );
        let market = deribit_market(&instrument);
        assert_eq!(market.as_ref(), "BTC-27MAY24-9000-P");
    }

    #[test]
    fn test_deribit_market_spot_fallback() {
        // Deribit doesn't have spot, so we map to perpetual
        let instrument = test_instrument("btc", "usd", Spot);
        let market = deribit_market(&instrument);
        assert_eq!(market.as_ref(), "BTC-PERPETUAL");
    }
}
