//! ingest: chain -> store. Everything that turns raw chain bytes
//! (chainsync, a bootstrap snapshot) into rows in the index.

pub mod catchup;
pub mod decode;
pub mod mempack;
pub mod seed;
pub mod store;
pub mod sync;
pub mod tvar;
