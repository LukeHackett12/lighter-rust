use crate::{
    config::LighterConfig,
    error::{LighterError, Result},
    signer::FFISigner,
};
use futures::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{collections::HashMap, str::FromStr};
use tokio::net::TcpStream;
use tokio_tungstenite::{connect_async, tungstenite::Message, MaybeTlsStream, WebSocketStream};

pub type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;

#[derive(Debug)]
enum AccountSubscription {
    AccountAll { account_id: String },
    AccountMarket { market_id: String, account_id: String },
}

impl AccountSubscription {
    fn parse(raw: &str) -> Result<Self> {
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            return Err(LighterError::Config(
                "Account subscription identifier cannot be empty".into(),
            ));
        }

        let normalized = trimmed.trim_matches('/');
        if normalized.is_empty() {
            return Err(LighterError::Config(format!(
                "Invalid account subscription `{trimmed}`"
            )));
        }

        if let Some(rest) = normalized.strip_prefix("account_market/") {
            return Self::parse_market(rest, trimmed);
        }

        if let Some(rest) = normalized.strip_prefix("account_all/") {
            return Self::parse_account(rest, trimmed);
        }

        let mut segments = normalized.split('/');
        let first = segments.next().unwrap_or_default().trim();
        let second = segments.next();

        if let Some(second_part) = second {
            if segments.next().is_some() {
                return Err(LighterError::Config(format!(
                    "Invalid account subscription `{trimmed}`"
                )));
            }
            return Self::parse_market_parts(first, second_part.trim(), trimmed);
        }

        Self::parse_account(first, trimmed)
    }

    fn parse_account(segment: &str, raw: &str) -> Result<Self> {
        let account_id = segment.trim();
        if account_id.is_empty() {
            return Err(LighterError::Config(format!(
                "Invalid account subscription `{raw}`: missing account id"
            )));
        }

        Ok(Self::AccountAll {
            account_id: account_id.to_string(),
        })
    }

    fn parse_market(segment: &str, raw: &str) -> Result<Self> {
        let mut parts = segment.split('/');
        let market_id = parts.next().map(str::trim).unwrap_or_default();
        let account_id = parts.next().map(str::trim).unwrap_or_default();

        if parts.next().is_some() {
            return Err(LighterError::Config(format!(
                "Invalid account market subscription `{raw}`"
            )));
        }

        Self::parse_market_parts(market_id, account_id, raw)
    }

    fn parse_market_parts(market_id: &str, account_id: &str, raw: &str) -> Result<Self> {
        if market_id.is_empty() || account_id.is_empty() {
            return Err(LighterError::Config(format!(
                "Invalid account market subscription `{raw}`"
            )));
        }

        Ok(Self::AccountMarket {
            market_id: market_id.to_string(),
            account_id: account_id.to_string(),
        })
    }
}

#[derive(Debug, Clone)]
struct AccountChannelMeta {
    key: String,
    requires_auth: bool,
}

impl AccountChannelMeta {
    fn new(key: String, requires_auth: bool) -> Self {
        Self {
            key,
            requires_auth,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WsRequest {
    pub method: String,
    pub params: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WsResponse {
    pub id: Option<String>,
    pub result: Option<Value>,
    pub error: Option<WsError>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WsError {
    pub code: i32,
    pub message: String,
    pub data: Option<Value>,
}

#[derive(Debug, strum::Display, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
enum WsMessage {
    Connected,
    #[serde(rename = "subscribed/order_book")]
    SubscribedOrderBook,
    #[serde(rename = "update/order_book")]
    UpdateOrderBook,
    #[serde(rename = "subscribed/account_all")]
    SubscribedAccountAll,
    #[serde(rename = "update/account_all")]
    UpdateAccountAll,
    #[serde(rename = "subscribed/account_market")]
    SubscribedAccountMarket,
    #[serde(rename = "update/account_market")]
    UpdateAccountMarket,
    #[serde(rename = "subscribed/trade")]
    SubscribedTrade,
    #[serde(rename = "update/trade")]
    UpdateTrade,
    Ping,
}

#[derive(Debug, PartialEq, Eq, Hash)]
enum WsSubscriptionType {
    OrderBooks,
    Accounts,
    Trades,
}

// #[derive(Debug)]
// struct WsSubscription {
//     id: uuid::Uuid,
//     pub state: Option<HashMap<String, Value>>,
// }

#[derive(Debug)]
pub struct WsClient<F1, F2, F3>
where
    F1: Fn(String, Value) + Send + Sync + 'static,
    F2: Fn(String, Value) + Send + Sync + 'static,
    F3: Fn(String, Value) + Send + Sync + 'static,
{
    stream: WsStream,
    subscriptions: HashMap<WsSubscriptionType, HashMap<String, Option<Value>>>,
    account_channels: HashMap<String, AccountChannelMeta>,
    signer: Option<FFISigner>,
    on_order_book_update: Option<F1>,
    on_account_update: Option<F2>,
    on_trade_update: Option<F3>,
    // {OrderBook: {'1': {id, state}}}}
}

pub struct WsClientBuilder<F1, F2, F3>
where
    F1: Fn(String, Value) + Send + Sync + 'static,
    F2: Fn(String, Value) + Send + Sync + 'static,
    F3: Fn(String, Value) + Send + Sync + 'static,
{
    config: Option<LighterConfig>,
    order_books_subs: Option<(Vec<String>, F1)>,
    accounts_subs: Option<(Vec<String>, F2)>,
    trade_subs: Option<(Vec<String>, F3)>,
}

impl<F1, F2, F3> WsClientBuilder<F1, F2, F3>
where
    F1: Fn(String, Value) + Send + Sync + 'static,
    F2: Fn(String, Value) + Send + Sync + 'static,
    F3: Fn(String, Value) + Send + Sync + 'static,
{
    pub fn with_config(mut self, config: LighterConfig) -> Self {
        self.config = Some(config);
        self
    }

    pub fn with_order_books_subs(
        mut self,
        order_book_subs: Vec<String>,
        order_book_update_fn: F1,
    ) -> Self {
        self.order_books_subs = Some((order_book_subs, order_book_update_fn));
        self
    }

    /// Registers account subscriptions.
    ///
    /// Entries can be plain account identifiers (e.g. `40` or `account_all/40`) or
    /// market-specific tuples (e.g. `0/40` or `account_market/0/40`).
    pub fn with_accounts_subs(mut self, account_subs: Vec<String>, account_update_fn: F2) -> Self {
        self.accounts_subs = Some((account_subs, account_update_fn));
        self
    }

    pub fn with_trade_subs(mut self, trade_subs: Vec<String>, trade_update_fn: F3) -> Self {
        self.trade_subs = Some((trade_subs, trade_update_fn));
        self
    }

    pub async fn build(self) -> Result<WsClient<F1, F2, F3>> {
        let config = self.config.unwrap_or_default();

        if self.accounts_subs.is_none()
            && self.order_books_subs.is_none()
            && self.trade_subs.is_none()
        {
            return Err(LighterError::Generic("No subscriptions provided".into()));
        }

        // create client
        let (ws_stream, _) = connect_async(&config.ws_url)
            .await
            .map_err(|e| LighterError::WebSocket(Box::new(e)))?;

        let mut subs = HashMap::new();
        let mut account_channels = HashMap::new();
        let mut needs_auth_token = false;

        let mut on_order_book_update = None;
        if let Some((order_books_subs, handler)) = self.order_books_subs {
            let order_books_subs = order_books_subs
                .iter()
                .map(|v| (v.to_string(), None))
                .collect::<HashMap<_, _>>();
            subs.insert(WsSubscriptionType::OrderBooks, order_books_subs);
            on_order_book_update = Some(handler);
        }

        let mut on_account_update = None;
        if let Some((account_subs, handler)) = self.accounts_subs {
            if account_subs.is_empty() {
                return Err(LighterError::Config(
                    "Expected at least one account subscription".into(),
                ));
            }

            let mut account_states = HashMap::new();

            for raw_sub in account_subs {
                let subscription = AccountSubscription::parse(&raw_sub)?;
                match subscription {
                    AccountSubscription::AccountAll { account_id } => {
                        let key = account_id.clone();
                        let channel = format!("account_all/{account_id}");
                        account_channels
                            .entry(channel)
                            .or_insert_with(|| AccountChannelMeta::new(key.clone(), true));
                        account_states.entry(key).or_insert(None);
                        needs_auth_token = true;
                    }
                    AccountSubscription::AccountMarket {
                        market_id,
                        account_id,
                    } => {
                        let key = format!("{market_id}/{account_id}");
                        let channel = format!("account_market/{market_id}/{account_id}");
                        account_channels
                            .entry(channel)
                            .or_insert_with(|| AccountChannelMeta::new(key.clone(), true));
                        account_states.entry(key).or_insert(None);
                        needs_auth_token = true;
                    }
                }
            }

            if account_states.is_empty() {
                return Err(LighterError::Config(
                    "No valid account subscriptions provided".into(),
                ));
            }

            subs.insert(WsSubscriptionType::Accounts, account_states);
            on_account_update = Some(handler);
        }

        let mut on_trade_update = None;
        if let Some((trade_subs, handler)) = self.trade_subs {
            let trade_subs = trade_subs
                .iter()
                .map(|v| (v.to_string(), None))
                .collect::<HashMap<_, _>>();
            subs.insert(WsSubscriptionType::Trades, trade_subs);
            on_trade_update = Some(handler);
        }

        let signer = if needs_auth_token {
            Some(FFISigner::try_from(&config)?)
        } else {
            None
        };

        Ok(WsClient {
            stream: ws_stream,
            subscriptions: subs,
            account_channels,
            signer,
            on_order_book_update,
            on_account_update,
            on_trade_update,
        })
    }
}

impl<F1, F2, F3> WsClient<F1, F2, F3>
where
    F1: Fn(String, Value) + Send + Sync + 'static,
    F2: Fn(String, Value) + Send + Sync + 'static,
    F3: Fn(String, Value) + Send + Sync + 'static,
{
    pub fn builder() -> WsClientBuilder<F1, F2, F3> {
        WsClientBuilder {
            config: None,
            order_books_subs: None,
            accounts_subs: None,
            trade_subs: None,
        }
    }

    pub async fn run(&mut self) -> Result<()> {
        while let Some(msg) = self.stream.next().await {
            let msg = msg.map_err(|e| {
                tracing::error!("unable to get message: {e}");
                LighterError::Generic("Unable to handle message".into())
            })?;

            self.handle_message(msg).await?;
        }

        Ok(())
    }

    // TODO: complete
    async fn handle_message(&mut self, msg: Message) -> Result<()> {
        match msg {
            Message::Text(data) => {
                let msg = serde_json::from_str::<Value>(&data).map_err(|e| {
                    tracing::error!("unable to deserialize msg: {e}");
                    LighterError::Generic("Unable to deserialize json message".into())
                })?;

                if let Some(msg_type) = msg.get("type") {
                    let api_msg = serde_json::from_value::<WsMessage>(msg_type.to_owned())
                        .map_err(|e| {
                            tracing::error!("unable to deserialize api msg: {e}");
                            LighterError::Generic("Unable to deserialize api message".into())
                        })?;

                    match api_msg {
                        WsMessage::Connected => self.handle_connected().await?,
                        WsMessage::SubscribedOrderBook => {
                            self.handle_subscribed_order_book(msg).await?
                        }
                        WsMessage::UpdateOrderBook => self.handle_update_order_book(msg).await?,
                        WsMessage::SubscribedAccountAll => {
                            self.handle_subscribed_account_all(msg).await?
                        }
                        WsMessage::UpdateAccountAll => {
                            self.handle_update_account_all(msg).await?
                        }
                        WsMessage::SubscribedAccountMarket => {
                            self.handle_subscribed_account_market(msg).await?
                        }
                        WsMessage::UpdateAccountMarket => {
                            self.handle_update_account_market(msg).await?
                        }
                        WsMessage::SubscribedTrade | WsMessage::UpdateTrade => {
                            self.handle_trade_message(msg).await?
                        }
                        WsMessage::Ping => self
                            .stream
                            .send(Message::text(json!({"type": "pong"}).to_string()))
                            .await
                            .map_err(|e| {
                                tracing::error!("unable to send `pong`");
                                LighterError::Generic("Unable to send `pong`".into())
                            })?,
                    }
                }

                Ok(())
            }
            _ => Ok(()),
        }
    }

    async fn handle_connected(&mut self) -> Result<()> {
        let order_book_subs = self.subscriptions.get(&WsSubscriptionType::OrderBooks);
        let trade_subs = self.subscriptions.get(&WsSubscriptionType::Trades);

        if let Some(order_books_subs) = order_book_subs {
            for market_id in order_books_subs.keys() {
                let resp =
                    json!({"type": "subscribe", "channel": format!("order_book/{market_id}")});
                self.stream
                    .send(Message::text(resp.to_string()))
                    .await
                    .map_err(|e| {
                        tracing::error!("unable to send `connected` response: {}", e);
                        LighterError::Generic(format!(
                            "Unable to send `connected` response: {e}"
                        ))
                    })?;
            }
        }

        if !self.account_channels.is_empty() {
            let channels: Vec<(String, bool)> = self
                .account_channels
                .iter()
                .map(|(channel, meta)| (channel.clone(), meta.requires_auth))
                .collect();
            let mut cached_auth: Option<String> = None;

            for (channel, requires_auth) in channels {
                let mut payload = json!({"type": "subscribe", "channel": channel});

                if requires_auth {
                    let signer = self.signer.as_ref().ok_or_else(|| {
                        tracing::error!("account market subscriptions require auth");
                        LighterError::Auth(
                            "Account market subscriptions require authenticated config".into(),
                        )
                    })?;

                    let token = if let Some(token) = cached_auth.clone() {
                        token
                    } else {
                        let fresh = signer.get_auth_token(None)?;
                        cached_auth = Some(fresh.clone());
                        fresh
                    };

                    if let Some(obj) = payload.as_object_mut() {
                        obj.insert("auth".into(), Value::String(token));
                    }
                }

                self.stream
                    .send(Message::text(payload.to_string()))
                    .await
                    .map_err(|e| {
                        tracing::error!("unable to send `connected` response: {}", e);
                        LighterError::Generic(format!(
                            "Unable to send `connected` response: {e}"
                        ))
                    })?;
            }
        }

        if let Some(trade_subs) = trade_subs {
            for market_id in trade_subs.keys() {
                let resp = json!({"type": "subscribe", "channel": format!("trade/{market_id}")});
                self.stream
                    .send(Message::text(resp.to_string()))
                    .await
                    .map_err(|e| {
                        tracing::error!("unable to send `connected` response: {}", e);
                        LighterError::Generic(format!(
                            "Unable to send `connected` response: {e}"
                        ))
                    })?;
            }
        }

        Ok(())
    }

    async fn handle_subscribed_order_book(&mut self, msg: Value) -> Result<()> {
        let market_id = Self::extract_market_id(&msg)?;
        let order_book = msg
            .get("order_book")
            .cloned()
            .ok_or_else(|| {
                tracing::error!("unable to get order_book from message");
                LighterError::Generic("Unable to get `order_book` from message".into())
            })?;

        self.update_order_book_state(market_id, order_book)
    }

    async fn handle_update_order_book(&mut self, msg: Value) -> Result<()> {
        let market_id = Self::extract_market_id(&msg)?;
        let order_book = msg
            .get("order_book")
            .cloned()
            .ok_or_else(|| {
                tracing::error!("unable to get order_book update from message");
                LighterError::Generic("Unable to get `order_book` update from message".into())
            })?;

        self.update_order_book_state(market_id, order_book)
    }

    async fn handle_subscribed_account_all(&mut self, msg: Value) -> Result<()> {
        self.handle_account_message(msg)
    }

    async fn handle_update_account_all(&mut self, msg: Value) -> Result<()> {
        self.handle_account_message(msg)
    }

    async fn handle_subscribed_account_market(&mut self, msg: Value) -> Result<()> {
        self.handle_account_message(msg)
    }

    async fn handle_update_account_market(&mut self, msg: Value) -> Result<()> {
        self.handle_account_message(msg)
    }

    fn handle_account_message(&mut self, msg: Value) -> Result<()> {
        let channel = msg
            .get("channel")
            .and_then(|value| value.as_str())
            .ok_or_else(|| {
                tracing::error!("account message missing `channel`");
                LighterError::Generic("Account message missing `channel`".into())
            })?
            .to_string();

        let meta = self.account_channels.get(&channel);

        if let Some(meta) = meta {
            if let Some(accounts_subs) = self.subscriptions.get_mut(&WsSubscriptionType::Accounts) {
                accounts_subs.insert(meta.key.clone(), Some(msg.clone()));
            }
        }

        if let Some(handler) = &self.on_account_update {
            let key = meta
                .map(|meta| meta.key.clone())
                .unwrap_or_else(|| channel.clone());
            handler(key, msg);
        } else if meta.is_none() {
            tracing::warn!("received account payload for unsubscribed channel `{channel}`");
        }

        Ok(())
    }

    async fn handle_trade_message(&mut self, msg: Value) -> Result<()> {
        let market_id = Self::extract_market_id(&msg)?;
        if let Some(trade_subs) = self.subscriptions.get(&WsSubscriptionType::Trades) {
            if trade_subs.contains_key(&market_id) {
                if let Some(handler) = &self.on_trade_update {
                    handler(market_id, msg);
                }
            }
        }
        Ok(())
    }

    fn update_order_book_state(&mut self, market_id: String, order_book: Value) -> Result<()> {
        if let Some(order_book_subs) = self.subscriptions.get_mut(&WsSubscriptionType::OrderBooks) {
            let merged = if let Some(Some(existing)) = order_book_subs.get(&market_id) {
                Self::merge_order_books(existing.clone(), order_book)?
            } else {
                order_book
            };

            order_book_subs.insert(market_id.clone(), Some(merged.clone()));

            if let Some(handler) = &self.on_order_book_update {
                handler(market_id, merged);
            }
        }

        Ok(())
    }

    fn merge_order_books(mut existing: Value, update: Value) -> Result<Value> {
        let Some(existing_obj) = existing.as_object_mut() else {
            return Ok(update);
        };

        let Some(update_obj) = update.as_object() else {
            return Ok(update);
        };

        if let Some(new_asks) = update_obj.get("asks").and_then(|v| v.as_array()) {
            let current_asks = existing_obj
                .get("asks")
                .and_then(|v| v.as_array())
                .cloned()
                .unwrap_or_default();
            let merged_asks = Self::update_orders(new_asks, &current_asks, false)?;
            existing_obj.insert("asks".to_string(), Value::Array(merged_asks));
        } else if update_obj.contains_key("asks") {
            existing_obj.insert("asks".to_string(), update_obj["asks"].clone());
        }

        if let Some(new_bids) = update_obj.get("bids").and_then(|v| v.as_array()) {
            let current_bids = existing_obj
                .get("bids")
                .and_then(|v| v.as_array())
                .cloned()
                .unwrap_or_default();
            let merged_bids = Self::update_orders(new_bids, &current_bids, true)?;
            existing_obj.insert("bids".to_string(), Value::Array(merged_bids));
        } else if update_obj.contains_key("bids") {
            existing_obj.insert("bids".to_string(), update_obj["bids"].clone());
        }

        Ok(existing)
    }

    fn update_orders(
        new_orders: &[Value],
        existing_orders: &[Value],
        sort_desc: bool,
    ) -> Result<Vec<Value>> {
        let mut book: HashMap<String, Value> = existing_orders
            .iter()
            .filter_map(|order| {
                let price = order.get("price")?.as_str()?;
                Some((price.to_string(), order.clone()))
            })
            .collect();

        for order in new_orders {
            if let Some(price) = order.get("price").and_then(|v| v.as_str()) {
                if Self::is_zero_level(order) {
                    book.remove(price);
                } else {
                    book.insert(price.to_string(), order.clone());
                }
            }
        }

        let mut orders: Vec<(f64, Value)> = book
            .into_iter()
            .filter_map(|(price, order)| {
                f64::from_str(&price)
                    .ok()
                    .map(|parsed_price| (parsed_price, order))
            })
            .collect();

        orders.sort_by(|(price_a, _), (price_b, _)| {
            if sort_desc {
                price_b
                    .partial_cmp(price_a)
                    .unwrap_or(std::cmp::Ordering::Equal)
            } else {
                price_a
                    .partial_cmp(price_b)
                    .unwrap_or(std::cmp::Ordering::Equal)
            }
        });

        Ok(orders.into_iter().map(|(_, order)| order).collect())
    }

    fn is_zero_level(order: &Value) -> bool {
        const AMOUNT_FIELDS: [&str; 4] = [
            "remaining_base_amount",
            "size",
            "quantity",
            "qty",
        ];

        if let Some(action) = order.get("action").and_then(|v| v.as_str()) {
            if action.eq_ignore_ascii_case("delete") {
                return true;
            }
        }

        AMOUNT_FIELDS.iter().any(|field| {
            order
                .get(*field)
                .and_then(|value| {
                    if let Some(as_str) = value.as_str() {
                        f64::from_str(as_str).ok()
                    } else {
                        value.as_f64()
                    }
                })
                .map(|amount| amount == 0.0)
                .unwrap_or(false)
        })
    }

    fn extract_market_id(msg: &Value) -> Result<String> {
        let channel = msg
            .get("channel")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                tracing::error!("Unable to get channel from message");
                LighterError::Generic("Unable to get channel from message".into())
            })?;

        channel
            .split(|c| c == ':' || c == '/')
            .last()
            .filter(|id| !id.is_empty())
            .map(|id| id.to_string())
            .ok_or_else(|| {
                tracing::error!("Unable to get market_id");
                LighterError::Generic("Unable to get market_id".into())
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_account_all_inputs() {
        for input in ["40", "account_all/40", "/account_all/40/"] {
            match AccountSubscription::parse(input).expect("valid account id") {
                AccountSubscription::AccountAll { account_id } => assert_eq!(account_id, "40"),
                _ => panic!("expected account_all"),
            }
        }
    }

    #[test]
    fn parses_account_market_inputs() {
        for input in ["0/40", "account_market/0/40", "/account_market/0/40/"] {
            match AccountSubscription::parse(input).expect("valid account market") {
                AccountSubscription::AccountMarket {
                    market_id,
                    account_id,
                } => {
                    assert_eq!(market_id, "0");
                    assert_eq!(account_id, "40");
                }
                _ => panic!("expected account_market"),
            }
        }
    }

    #[test]
    fn rejects_invalid_inputs() {
        assert!(AccountSubscription::parse("account_market/0").is_err());
        assert!(AccountSubscription::parse("account_market//40").is_err());
        assert!(AccountSubscription::parse("").is_err());
    }
}
