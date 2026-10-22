//! Single-peer Cardano node-to-node client.
//!
//! This crate drives `amaru-protocols`' handshake and chainsync initiators
//! directly for **one configured bootstrap peer** -- the same role
//! `pallas_network::facades::PeerClient` plays in floe today, except with real
//! CIP-0164 pipelining (which pallas-network's lock-step `Client` cannot do).
//!
//! Deliberately **not** using `amaru-protocols::manager` *or*
//! `amaru-protocols::connection::Connection`: `manager` assumes a full
//! gossiping node (inbound accept, peer-sharing, tx-submission responders,
//! redial policy); `Connection`'s `do_initialize` hardcodes advertising
//! full-duplex capability, which would force us into running the responder
//! side too. [`orchestrator`] is a from-scratch, initiator-only replacement
//! for both, built directly on the public `register_chainsync_initiator`/
//! `handshake::initiator` pieces. See `plan-amaru-migration.md` in the floe
//! repo root for the full writeup of this decision.
//!
//! [`pipeline`] is the stage that actually drives CIP-0164 pipelining: the
//! chainsync initiator never sends more than one `RequestNext` on its own, it
//! waits to be told to by this stage.

// VOODOO COMPILER MAGIC
#![allow(incomplete_features)]
#![feature(generic_const_exprs, const_type_name, unsized_const_params, adt_const_params)]
#![recursion_limit = "512"]


pub mod client;
pub mod orchestrator;
pub mod pipeline;
pub mod resume;
