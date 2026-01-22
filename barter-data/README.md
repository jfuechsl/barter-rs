
# Barter-Data
A high-performance WebSocket integration library for streaming public market data from leading cryptocurrency 
exchanges - batteries included. It is:
* **Easy**: Barter-Data's simple StreamBuilder interface allows for easy & quick setup (see example below!).
* **Normalised**: Barter-Data's unified interface for consuming public WebSocket data means every Exchange returns a normalised data model.
* **Real-Time**: Barter-Data utilises real-time WebSocket integrations enabling the consumption of normalised tick-by-tick data.
* **Extensible**: Barter-Data is highly extensible, and therefore easy to contribute to with coding new integrations!

**See: [`Barter`], [`Barter-Instrument`], [`Barter-Execution`] & [`Barter-Integration`] for
comprehensive documentation of other Barter libraries.**

[![Crates.io][crates-badge]][crates-url]
[![MIT licensed][mit-badge]][mit-url]
[![Discord chat][discord-badge]][discord-url]

[crates-badge]: https://img.shields.io/crates/v/barter-data.svg
[crates-url]: https://crates.io/crates/barter-data

[mit-badge]: https://img.shields.io/badge/license-MIT-blue.svg
[mit-url]: https://gitlab.com/open-source-keir/financial-modelling/trading/barter-data-rs/-/blob/main/LICENCE

[discord-badge]: https://img.shields.io/discord/910237311332151317.svg?logo=discord&style=flat-square
[discord-url]: https://discord.gg/wE7RqhnQMV

[API Documentation] |
[Chat]

[`Barter`]: https://crates.io/crates/barter
[`Barter-Instrument`]: https://crates.io/crates/barter-instrument
[`Barter-Execution`]: https://crates.io/crates/barter-execution
[`Barter-Integration`]: https://crates.io/crates/barter-integration
[API Documentation]: https://docs.rs/barter-data/latest/barter_data
[Chat]: https://discord.gg/wE7RqhnQMV

## Overview
Barter-Data is a high-performance WebSocket integration library for streaming public market data from leading cryptocurrency 
exchanges. It presents an easy-to-use and extensible set of interfaces that can deliver normalised exchange data in real-time.

**Requires Rust 1.85+**

From a user perspective, the major component is the `StreamBuilder` structures that assists in initialising an 
arbitrary number of exchange `MarketStream`s using input `Subscription`s. Simply build your dream set of 
`MarketStreams` and `Barter-Data` will do the rest!

### Supported Exchange Subscriptions

|        Exchange         |         Constructor Code         |               InstrumentKinds               |                SubscriptionKinds                 |
| :---------------------: | :------------------------------: | :-----------------------------------------: | :----------------------------------------------: |
|     **BinanceSpot**     |     `BinanceSpot::default()`     |                    Spot                     | PublicTrades <br> OrderBooksL1 <br> OrderBooksL2 |
|  **BinanceFuturesUsd**  |  `BinanceFuturesUsd::default()`  |                  Perpetual                  | PublicTrades <br> OrderBooksL1 <br> OrderBooksL2 |
|      **Bitfinex**       |            `Bitfinex`            |                    Spot                     |                   PublicTrades                   |
|       **Bitmex**        |             `Bitmex`             |                  Perpetual                  |                   PublicTrades                   |
|      **BybitSpot**      |      `BybitSpot::default()`      |                    Spot                     |                   PublicTrades                   |
| **BybitPerpetualsUsd**  | `BybitPerpetualsUsd::default()`  |                  Perpetual                  |                   PublicTrades                   |
|      **Coinbase**       |            `Coinbase`            |                    Spot                     |                   PublicTrades                   |
|     **GateioSpot**      |     `GateioSpot::default()`      |                    Spot                     |                   PublicTrades                   |
|  **GateioFuturesUsd**   |  `GateioFuturesUsd::default()`   |                   Future                    |                   PublicTrades                   |
|  **GateioFuturesBtc**   |  `GateioFuturesBtc::default()`   |                   Future                    |                   PublicTrades                   |
| **GateioPerpetualsUsd** | `GateioPerpetualsUsd::default()` |                  Perpetual                  |                   PublicTrades                   |
| **GateioPerpetualsBtc** | `GateioPerpetualsBtc::default()` |                  Perpetual                  |                   PublicTrades                   |
|  **GateioOptionsBtc**   |    `GateioOptions::default()`    |                   Option                    |                   PublicTrades                   |
|       **Kraken**        |             `Kraken`             |                    Spot                     |          PublicTrades <br> OrderBooksL1          |
|         **Okx**         |              `Okx`               | Spot <br> Future <br> Perpetual <br> Option |                   PublicTrades                   |


## Examples
See barter-data-rs/examples for a more comprehensive selection of examples! 

### Multi Exchange Public Trades
```rust,no_run
use barter_data::{
    exchange::{
        binance::{futures::BinanceFuturesUsd, spot::BinanceSpot},
        bitmex::Bitmex,
        bybit::{futures::BybitPerpetualsUsd, spot::BybitSpot},
        coinbase::Coinbase,
        gateio::{
            option::GateioOptions,
            perpetual::{GateioPerpetualsBtc, GateioPerpetualsUsd},
            spot::GateioSpot,
        },
        okx::Okx,
    },
    streams::{Streams, reconnect::stream::ReconnectingStream},
    subscription::trade::PublicTrades,
};
use barter_integration::model::instrument::kind::{
    FutureContract, InstrumentKind, OptionContract, OptionExercise, OptionKind,
};
use chrono::{TimeZone, Utc};
use futures::StreamExt;

#[tokio::main]
async fn main() {
    // Initialise PublicTrades Streams for various exchanges
    // '--> each call to StreamBuilder::subscribe() creates a separate WebSocket connection
    let streams = Streams::<PublicTrades>::builder()
        .subscribe([
            (BinanceSpot::default(), "btc", "usdt", InstrumentKind::Spot, PublicTrades),
            (BinanceSpot::default(), "eth", "usdt", InstrumentKind::Spot, PublicTrades),
        ])
        .subscribe([
            (BinanceFuturesUsd::default(), "btc", "usdt", InstrumentKind::Perpetual, PublicTrades),
            (BinanceFuturesUsd::default(), "eth", "usdt", InstrumentKind::Perpetual, PublicTrades),
        ])
        .subscribe([
            (Coinbase, "btc", "usd", InstrumentKind::Spot, PublicTrades),
            (Coinbase, "eth", "usd", InstrumentKind::Spot, PublicTrades),
        ])
        .subscribe([
            (GateioSpot::default(), "btc", "usdt", InstrumentKind::Spot, PublicTrades),
        ])
        .subscribe([
            (GateioPerpetualsUsd::default(), "btc", "usdt", InstrumentKind::Perpetual, PublicTrades),
        ])
        .subscribe([
            (GateioPerpetualsBtc::default(), "btc", "usd", InstrumentKind::Perpetual, PublicTrades),
        ])
        .subscribe([
            (GateioOptions::default(), "btc", "usdt", InstrumentKind::Option(put_contract()), PublicTrades),
        ])
        .subscribe([
            (Okx, "btc", "usdt", InstrumentKind::Spot, PublicTrades),
            (Okx, "btc", "usdt", InstrumentKind::Perpetual, PublicTrades),
            (Okx, "btc", "usd", InstrumentKind::Future(future_contract()), PublicTrades),
            (Okx, "btc", "usd", InstrumentKind::Option(call_contract()), PublicTrades),
        ])
        .subscribe([
            (BybitSpot::default(), "btc", "usdt", InstrumentKind::Spot, PublicTrades),
            (BybitSpot::default(), "eth", "usdt", InstrumentKind::Spot, PublicTrades),
        ])
        .subscribe([
            (BybitPerpetualsUsd::default(), "btc", "usdt", InstrumentKind::Perpetual, PublicTrades),
        ])
        .subscribe([
            (Bitmex, "xbt", "usd", InstrumentKind::Perpetual, PublicTrades)
        ])
        .init()
        .await
        .unwrap();

    // Select and merge every exchange Stream using futures_util::stream::select_all
    // Note: use `Streams.select(ExchangeId)` to interact with individual exchange streams!
    let mut joined_stream = streams
        .select_all()
        .with_error_handler(|error| println!(format!("MarketStream generated error: {error:?}")));

    while let Some(event) = joined_stream.next().await {
        println!("{event:?}");
    }
}
```

## Getting Help
Firstly, see if the answer to your question can be found in the [API Documentation]. If the answer is not there, I'd be happy to help via [Chat] <br>
and try answer your question via Discord.

## Support Barter Development
Help us advance Barter's capabilities by becoming a sponsor (or supporting me with a tip!).

Your contribution will allow me to dedicate more time to Barter, accelerating feature development and improvements.

**Please email *justastream.code@gmail.com* for all inquiries**

Please see [here](../README.md#support-barter-development) for more information.

## Contributing
Thanks in advance for helping to develop the Barter ecosystem! Please do get hesitate to get touch via the Discord
[Chat] to discuss development, new features, and the future roadmap.

For comprehensive developer documentation on extending Barter-Data, see the [API Documentation] which includes
detailed guides on adding new exchanges and subscription kinds.

### Quick Start: Adding A New Exchange Connector

Follow these steps to add support for a new exchange. For detailed documentation, see the [developer guide](https://docs.rs/barter-data/latest/barter_data/developer_guide/index.html) in the API docs.

#### 1. Create Exchange Module Structure
```
src/exchange/my_exchange/
├── mod.rs           # Connector implementation
├── channel.rs       # Channel type for WebSocket channels
├── market.rs        # Market type for instrument identifiers
├── subscription.rs  # Subscription response validation
└── trade.rs         # PublicTrades implementation (example)
```

#### 2. Implement the Connector Trait
In `src/exchange/my_exchange/mod.rs`:
```rust
use crate::exchange::Connector;
use barter_instrument::exchange::ExchangeId;

#[derive(Clone, Default, Debug, Deserialize, Serialize)]
pub struct MyExchange;

impl Connector for MyExchange {
    const ID: ExchangeId = ExchangeId::MyExchange;  // Add to barter_instrument
    type Channel = MyExchangeChannel;
    type Market = MyExchangeMarket;
    type Subscriber = WebSocketSubscriber;
    type SubValidator = WebSocketSubValidator;
    type SubResponse = MyExchangeSubResponse;

    fn url() -> Result<Url, SocketError> {
        Url::parse("wss://api.myexchange.com/ws").map_err(SocketError::UrlParse)
    }

    fn requests(exchange_subs: Vec<ExchangeSub<Self::Channel, Self::Market>>) -> Vec<WsMessage> {
        // Build exchange-specific subscription request payloads
        vec![WsMessage::text(
            json!({ "method": "subscribe", "params": exchange_subs }).to_string()
        )]
    }
}
```

#### 3. Implement Channel and Market Types
Define how Barter subscriptions map to exchange-specific channels and markets:

**channel.rs:**
```rust
#[derive(Clone, Debug, Serialize)]
pub struct MyExchangeChannel(pub &'static str);

impl MyExchangeChannel {
    pub const TRADES: Self = Self("trades");
}

impl<Instrument> Identifier<MyExchangeChannel> for Subscription<MyExchange, Instrument, PublicTrades> {
    fn id(&self) -> MyExchangeChannel {
        MyExchangeChannel::TRADES
    }
}
```

**market.rs:**
```rust
#[derive(Clone, Debug, Serialize)]
pub struct MyExchangeMarket(pub String);

impl<Instrument> Identifier<MyExchangeMarket> for Subscription<MyExchange, Instrument, PublicTrades>
where
    Instrument: InstrumentData,
{
    fn id(&self) -> MyExchangeMarket {
        // Convert instrument to exchange format (e.g., "BTC-USDT")
        MyExchangeMarket(format!("{}-{}",
            self.instrument.base_asset().as_str().to_uppercase(),
            self.instrument.quote_asset().as_str().to_uppercase()
        ))
    }
}
```

#### 4. Implement StreamSelector for Subscription Kinds
For each supported subscription kind (e.g., `PublicTrades`), implement `StreamSelector`:
```rust
impl<Instrument> StreamSelector<Instrument, PublicTrades> for MyExchange
where
    Instrument: InstrumentData,
{
    type SnapFetcher = NoInitialSnapshots;
    type Stream = MyExchangeWsStream<StatelessTransformer<Self, Instrument::Key, PublicTrades, MyExchangeTrades>>;
}
```

#### 5. Create Exchange Message Types and Transformers
Define the exchange-specific message format and implement transformation to Barter's normalized types:
```rust
#[derive(Debug, Deserialize)]
pub struct MyExchangeTrades {
    pub subscription_id: SubscriptionId,
    pub data: Vec<MyExchangeTrade>,
}

impl<InstrumentKey: Clone> From<(ExchangeId, InstrumentKey, MyExchangeTrades)>
    for MarketIter<InstrumentKey, PublicTrade>
{
    fn from((exchange, instrument, trades): (ExchangeId, InstrumentKey, MyExchangeTrades)) -> Self {
        trades.data.into_iter().map(|trade| {
            Ok(MarketEvent {
                time_exchange: trade.timestamp,
                time_received: Utc::now(),
                exchange,
                instrument: instrument.clone(),
                kind: PublicTrade {
                    id: trade.id,
                    price: trade.price,
                    amount: trade.quantity,
                    side: trade.side,
                },
            })
        }).collect()
    }
}
```

#### 6. Add Tests and Examples
- Add unit tests for deserialization and transformation
- Create an example in `examples/my_exchange_trades.rs`
- Test with real exchange data

See the [complete developer guide](https://docs.rs/barter-data/latest/barter_data/developer_guide/index.html) for more details, including:
- Adding new subscription kinds to existing exchanges
- Implementing stateful transformers for OrderBooks
- Custom snapshot fetchers for L2/L3 order books
- Handling exchange-specific edge cases

### Adding A New SubscriptionKind

See the [developer guide on adding subscription kinds](https://docs.rs/barter-data/latest/barter_data/developer_guide/adding_subscription_kinds/index.html) for comprehensive instructions.

### Licence
This project is licensed under the [MIT license].

[MIT license]: https://github.com/barter-rs/barter-rs/blob/develop/LICENSE

### Contribution License Agreement

Any contribution you intentionally submit for inclusion in Barter workspace crates shall be:
1. Licensed under MIT
2. Subject to all disclaimers and limitations of liability stated below
3. Provided without any additional terms or conditions
4. Submitted with the understanding that the educational-only purpose and risk warnings apply

By submitting a contribution, you certify that you have the right to do so under these terms.

## LEGAL DISCLAIMER AND LIMITATION OF LIABILITY

PLEASE READ THIS DISCLAIMER CAREFULLY BEFORE USING THE SOFTWARE. BY ACCESSING OR USING THE SOFTWARE, YOU ACKNOWLEDGE AND AGREE TO BE BOUND BY THE TERMS HEREIN.

1. EDUCATIONAL PURPOSE
   This software and related documentation ("Software") are provided solely for educational and research purposes. The Software is not intended, designed, tested, verified or certified for commercial deployment, live trading, or production use of any kind.

2. NO FINANCIAL ADVICE
   Nothing contained in the Software constitutes financial, investment, legal, or tax advice. No aspect of the Software should be relied upon for trading decisions or financial planning. Users are strongly advised to consult qualified professionals for investment guidance specific to their circumstances.

3. ASSUMPTION OF RISK
   Trading in financial markets, including but not limited to cryptocurrencies, securities, derivatives, and other financial instruments, carries substantial risk of loss. Users acknowledge that:
   a) They may lose their entire investment;
   b) Past performance does not indicate future results;
   c) Hypothetical or simulated performance results have inherent limitations and biases.

4. DISCLAIMER OF WARRANTIES
   THE SOFTWARE IS PROVIDED "AS IS" WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED. TO THE MAXIMUM EXTENT PERMITTED BY LAW, THE AUTHORS AND COPYRIGHT HOLDERS EXPRESSLY DISCLAIM ALL WARRANTIES, INCLUDING BUT NOT LIMITED TO:
   a) MERCHANTABILITY
   b) FITNESS FOR A PARTICULAR PURPOSE
   c) NON-INFRINGEMENT
   d) ACCURACY OR RELIABILITY OF RESULTS
   e) SYSTEM INTEGRATION
   f) QUIET ENJOYMENT

5. LIMITATION OF LIABILITY
   IN NO EVENT SHALL THE AUTHORS, COPYRIGHT HOLDERS, CONTRIBUTORS, OR ANY AFFILIATED PARTIES BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING BUT NOT LIMITED TO PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES, LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

6. REGULATORY COMPLIANCE
   The Software is not registered with, endorsed by, or approved by any financial regulatory authority. Users are solely responsible for:
   a) Determining whether their use complies with applicable laws and regulations
   b) Obtaining any required licenses, permits, or registrations
   c) Meeting any regulatory obligations in their jurisdiction

7. INDEMNIFICATION
   Users agree to indemnify, defend, and hold harmless the authors, copyright holders, and any affiliated parties from and against any claims, liabilities, damages, losses, and expenses arising from their use of the Software.

8. ACKNOWLEDGMENT
   BY USING THE SOFTWARE, USERS ACKNOWLEDGE THAT THEY HAVE READ THIS DISCLAIMER, UNDERSTOOD IT, AND AGREE TO BE BOUND BY ITS TERMS AND CONDITIONS.

THE ABOVE LIMITATIONS MAY NOT APPLY IN JURISDICTIONS THAT DO NOT ALLOW THE EXCLUSION OF CERTAIN WARRANTIES OR LIMITATIONS OF LIABILITY.
