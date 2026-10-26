//! Single-peer Cardano node-to-node client: drives `amaru-protocols`'
//! handshake/chainsync/blockfetch initiators for one fixed peer, with real
//! CIP-0164 pipelining (unlike `pallas_network`'s lock-step `Client`).
//!
//! Deliberately not `amaru-protocols::manager`/`Connection`: those assume a
//! full gossiping node (peer-sharing, responders, redial). [`connection`] is
//! a from-scratch, initiator-only equivalent -- see its own module docs.
//!
//! [`pipeline`] drives the actual pipelining: the chainsync initiator only
//! ever sends one `RequestNext` on its own; this stage tells it to send more.

// VOODOO COMPILER MAGIC
#![allow(incomplete_features)]
// #![feature(generic_const_exprs, const_type_name, unsized_const_params, adt_const_params)]
#![feature(generic_const_exprs)]

pub mod client;
pub mod connection;
pub mod error;
pub mod pipeline;
pub mod resume;
pub mod single_fetch;

pub use error::{Error, Result};
