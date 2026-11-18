//! Local Redshift tools with database-owned permissions and bounded MCP results.
//!
//! The AWS adapter owns transport, the controller owns query handles and cursors,
//! and the server enforces tool visibility. No listener or remote MCP auth is used.

pub mod aws;
pub mod backend;
pub mod config;
pub mod controller;
pub mod error;
pub mod server;
pub mod sql;

pub use config::Config;
pub use server::RedshiftServer;
