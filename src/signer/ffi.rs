use chrono::{Duration, Utc};
use secrecy::{ExposeSecret, SecretString};

use crate::error::{LighterError, Result};
use crate::signer::data::TxData;
use std::ffi::{c_int, c_longlong, CStr, CString};
use std::sync::{Arc, RwLock};

pub mod ffisigner {
    #![allow(warnings)]
    include!(concat!(env!("OUT_DIR"), "/bindings.rs"));
}

#[derive(Debug, Clone)]
pub struct AuthToken {
    pub token: String,
    pub expiration: i64,
}

impl AuthToken {
    pub fn is_expired(&self) -> bool {
        Utc::now().timestamp() >= self.expiration
    }
}

#[derive(Debug)]
pub struct FFISigner {
    url: String,
    private_key: String,
    chain_id: c_int,
    api_key_index: c_int,
    account_index: c_int,
    // We should expect more reads than actual writes since the token will most likely have a long expiration
    // In case this does not happen, the implementation could be changed to have a Mutex
    // By using the Arc we ensure to have interior mutability
    auth_token: Arc<RwLock<Option<AuthToken>>>,
}

impl FFISigner {
    pub fn new(
        url: &str,
        private_key: SecretString,
        api_key_index: i32,
        account_index: i32,
    ) -> Result<Self> {
        let chain_id = if url.contains("mainnet") { 304 } else { 300 };
        let clean_key = private_key.expose_secret().trim_start_matches("0x");

        let signer = Self {
            url: url.to_string(),
            private_key: clean_key.to_string(),
            chain_id: chain_id as c_int,
            api_key_index: api_key_index as c_int,
            account_index: account_index as c_int,
            auth_token: Arc::new(RwLock::new(None)),
        };

        signer.create_client()?;
        Ok(signer)
    }

    pub fn get_tx_data(&self, data: TxData, nonce: i64) -> Result<String> {
        let res = match data {
            TxData::ChangePubKey(data) => {
                let c_pubkey = CString::new(data.new_pubk.as_str())
                    .map_err(|_| LighterError::Signing("Invalid key".to_string()))?;
                unsafe { ffisigner::SignChangePubKey(c_pubkey.as_ptr() as *mut i8, nonce) }
            }
            TxData::CreateOrder(data) => unsafe {
                ffisigner::SignCreateOrder(
                    data.market_index,
                    data.client_order_index,
                    data.base_amount,
                    data.price,
                    data.is_ask as c_int,
                    data.order_type as c_int,
                    data.time_in_force as c_int,
                    data.reduce_only as c_int,
                    data.trigger_price,
                    data.order_expiry as c_longlong,
                    nonce,
                )
            },
            TxData::SignCreateGroupedOrders(mut data) => {
                let orders_len = data.orders.len();
                let orders_ptr = data.orders.as_mut_ptr();
                unsafe {
                    ffisigner::SignCreateGroupedOrders(
                        data.grouping_type as u8,
                        orders_ptr,
                        orders_len as i32,
                        nonce,
                    )
                }
            }
            TxData::SignCancelOrder(data) => unsafe {
                ffisigner::SignCancelOrder(data.market_index, data.order_index, nonce)
            },
            TxData::SignWithdraw(data) => unsafe {
                ffisigner::SignWithdraw(data.usdc_amount, nonce)
            },
            TxData::SignCreateSubaccount => unsafe { ffisigner::SignCreateSubAccount(nonce) },
            TxData::SignCancelAllOrders(data) => unsafe {
                ffisigner::SignCancelAllOrders(data.time_in_force as c_int, data.time, nonce)
            },
            TxData::SignModifyOrder(data) => unsafe {
                ffisigner::SignModifyOrder(
                    data.market_index,
                    data.order_index,
                    data.amount,
                    data.price,
                    data.trigger_price,
                    nonce,
                )
            },
            TxData::SignTransfer(data) => unsafe {
                let memo = str::from_utf8(&data.memo)
                    .map_err(|_| LighterError::Generic("Invalid memo (non UTF-8)".to_string()))?;
                let memo = CString::new(memo)
                    .map_err(|_| LighterError::Signing("Invalid memo".to_string()))?;
                ffisigner::SignTransfer(
                    data.to_account_index,
                    data.usdc_amount,
                    data.fee,
                    memo.as_ptr() as *mut i8,
                    nonce,
                )
            },
            TxData::SignCreatePublicPool(data) => unsafe {
                ffisigner::SignCreatePublicPool(
                    data.operator_fee,
                    data.initial_total_shares,
                    data.min_operator_share_rate,
                    nonce,
                )
            },
            TxData::SignUpdatePublicPool(data) => unsafe {
                ffisigner::SignUpdatePublicPool(
                    data.public_pool_index,
                    data.status,
                    data.operator_fee,
                    data.min_operator_share_rate,
                    nonce,
                )
            },
            TxData::SignMintShares(data) => unsafe {
                ffisigner::SignMintShares(data.public_pool_index, data.share_amount, nonce)
            },
            TxData::SignBurnShares(data) => unsafe {
                ffisigner::SignBurnShares(data.public_pool_index, data.share_amount, nonce)
            },
            TxData::SignUpdateLeverage(data) => unsafe {
                ffisigner::SignUpdateLeverage(
                    data.market_index,
                    data.initial_margin_fraction,
                    data.margin_mode,
                    nonce,
                )
            },
            TxData::SignUpdateMargin(data) => unsafe {
                ffisigner::SignUpdateMargin(
                    data.market_index,
                    data.usdc_amount,
                    data.direction,
                    nonce,
                )
            },
        };

        self.parse_result(res)
    }

    pub fn get_auth_token(&self, expiration_timestamp: Option<i64>) -> Result<String> {
        {
            let guard = self.auth_token.read().map_err(|e| {
                tracing::error!("unable to get token read lock: {e}");
                LighterError::Generic("Unable to get auth token".into())
            })?;
            if let Some(auth_token) = &*guard {
                if !auth_token.is_expired() {
                    return Ok(auth_token.token.clone());
                }
            }
        }

        // not present/not valid anymore
        let new_token = self.create_auth_token_with_expiry(expiration_timestamp)?;
        let token_str = new_token.token.clone();
        let mut guard = self.auth_token.write().map_err(|e| {
            tracing::error!("unable to get token write lock: {e}");
            LighterError::Generic("Unable to get auth token".into())
        })?;
        *guard = Some(new_token);

        Ok(token_str)
    }

    fn create_auth_token_with_expiry(&self, deadline: Option<i64>) -> Result<AuthToken> {
        unsafe {
            let deadline =
                deadline.unwrap_or((chrono::Utc::now() + Duration::minutes(10)).timestamp());

            let result = ffisigner::CreateAuthToken(deadline);
            let token = self.parse_result(result)?;

            Ok(AuthToken {
                token,
                expiration: deadline,
            })
        }
    }

    fn create_client(&self) -> Result<()> {
        unsafe {
            let c_url = CString::new(self.url.as_str())
                .map_err(|_| LighterError::Signing("Invalid URL".to_string()))?;
            let c_key = CString::new(self.private_key.as_str())
                .map_err(|_| LighterError::Signing("Invalid key".to_string()))?;

            let res = ffisigner::CreateClient(
                c_url.as_ptr() as *mut i8,
                c_key.as_ptr() as *mut i8,
                self.chain_id as c_int,
                self.api_key_index as c_int,
                self.account_index as c_longlong,
            );

            if !res.is_null() {
                let err_str = CStr::from_ptr(res).to_string_lossy().to_string();
                libc::free(res as *mut libc::c_void);
                return Err(LighterError::Signing(err_str));
            }

            Ok(())
        }
    }

    fn parse_result(&self, result: ffisigner::StrOrErr) -> Result<String> {
        unsafe {
            if !result.err.is_null() {
                let error_str = CStr::from_ptr(result.err).to_string_lossy().to_string();
                libc::free(result.err as *mut libc::c_void);
                if !result.str_.is_null() {
                    libc::free(result.str_ as *mut libc::c_void);
                }
                return Err(LighterError::Signing(error_str));
            }

            if result.str_.is_null() {
                return Err(LighterError::Signing("Null result".to_string()));
            }

            let value_str = CStr::from_ptr(result.str_).to_string_lossy().to_string();
            libc::free(result.str_ as *mut libc::c_void);

            Ok(value_str)
        }
    }
}

#[cfg(test)]
mod tests {
    use secrecy::SecretString;

    use crate::signer::ffi::FFISigner;

    #[test]
    fn test_create_auth_token_testnet() {
        let signer = FFISigner::new(
            "https://testnet.zklighter.elliot.ai",
            SecretString::from(
                "12345678123456781234567812345678123456781234567812345678123456781234567812345678",
            ),
            3,
            2,
        )
        .unwrap();

        let token = signer.create_auth_token_with_expiry(None).unwrap();
        println!("Token: {token:?}");
    }

    #[test]
    fn test_create_auth_token_mainnet() {
        let signer = FFISigner::new(
            "https://mainnet.zklighter.elliot.ai",
            SecretString::from(
                "12345678123456781234567812345678123456781234567812345678123456781234567812345678",
            ),
            3,
            2,
        )
        .unwrap();

        let token = signer.create_auth_token_with_expiry(None).unwrap();
        println!("Token: {token:?}");
    }
}
