//! # Lighter Rust SDK
//!
//! A Rust SDK for the Lighter trading platform, providing async API access to trading operations,
//! account management, and real-time market data.
//!
//! ## Features
//!
//! - **Async/await support** - Built with Tokio for high-performance async operations
//! - **Type-safe API** - Comprehensive Rust types for all API responses
//! - **Pluggable APIs** - Support for REST APIs exposed by the Lighter platform, which can be enabled all or only a specific subset of them.
//! - **Ethereum signing** - Built-in support for Ethereum-compatible wallet signing
//!
//! ## Quick Start
//!
//! ```ignore
//! use lighter_rust::{LighterClient, Config, api::account::AccountBy};
//!
//! #[tokio::main]
//! async fn main() -> Result<()> {
//!     let config = LighterConfig::new()
//!         .with_api_key_private(YOUR_API_KEY_PRIVATE)
//!         .with_account_index(YOUR_ACCOUNT_INDEX)
//!         .with_api_key_index(YOUR_API_KEY_INDEX);
//!
//!     let client = HttpClient::builder()
//!         .with_config(config)
//!         .with_account()
//!         .build()?;
//!
//!     let account = client
//!         .api()
//!         .account()?
//!         .account(AccountBy::L1Address, YOUR_ACCOUNT_ADDRESS)
//!         .await?;
//!     println!("Account: {:?}", account);
//!     
//!     Ok(())
//! }
//! ```
//!
pub mod api; // implementation of the APIs interfaces
mod apis; // openapi generated
mod client; // module containing http/ws clients
mod config;
pub mod log;
pub mod models; // openapi generated, needed for requests/responses
pub mod signer; // module containing the interface to the `lighter-go` lib, used for signing
pub use config::LighterConfig;
mod error;
pub use error::{LighterError, Result};

pub use crate::{
    client::{HttpClient, WsClient},
    signer::Signer,
};
