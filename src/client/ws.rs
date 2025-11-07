use crate::{
    config::LighterConfig,
    error::{LighterError, Result},
};
use futures::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{collections::HashMap, str::FromStr};
use tokio::net::TcpStream;
use tokio_tungstenite::{connect_async, tungstenite::Message, MaybeTlsStream, WebSocketStream};

pub type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;

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
            let account_subs = account_subs
                .iter()
                .map(|v| (v.to_string(), None))
                .collect::<HashMap<_, _>>();
            subs.insert(WsSubscriptionType::Accounts, account_subs);
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

        Ok(WsClient {
            stream: ws_stream,
            subscriptions: subs,
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
                            self.handle_subscribed_account_all().await?
                        }
                        WsMessage::UpdateAccountAll => self.handle_update_account_all().await?,
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
        let accounts_subs = self.subscriptions.get(&WsSubscriptionType::Accounts);
        let trade_subs = self.subscriptions.get(&WsSubscriptionType::Trades);

        if let Some(order_books_subs) = order_book_subs {
            for market_id in order_books_subs.keys() {
                let resp =
                    json!({"type": "subscribe", "channel": format!("order_book/{market_id}")});
                self.stream
                    .send(Message::text(resp.to_string()))
                    .await
                    .map_err(|e| {
                        tracing::error!("unable to send `connected` response: {e}");
                        LighterError::Generic("Unable to send `connected` response: {e}".into())
                    })?;
            }
        }

        if let Some(accounts_subs) = accounts_subs {
            for account_id in accounts_subs.keys() {
                let resp =
                    json!({"type": "subscribe", "channel": format!("account_all/{account_id}")});
                self.stream
                    .send(Message::text(resp.to_string()))
                    .await
                    .map_err(|e| {
                        tracing::error!("unable to send `connected` response: {e}");
                        LighterError::Generic("Unable to send `connected` response: {e}".into())
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
                        tracing::error!("unable to send `connected` response: {e}");
                        LighterError::Generic("Unable to send `connected` response: {e}".into())
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

    async fn handle_subscribed_account_all(&mut self) -> Result<()> {
        Ok(())
    }

    async fn handle_update_account_all(&mut self) -> Result<()> {
        Ok(())
    }

    // async fn handle_ping(&mut self) -> Result<()> {
    //     Ok(())
    // }

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
