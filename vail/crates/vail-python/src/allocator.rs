//! Rust global allocator selection, without process-wide C interposition.

#[cfg(all(feature = "allocator-mimalloc", feature = "allocator-jemalloc"))]
compile_error!("select at most one allocator");

#[cfg(feature = "allocator-mimalloc")]
use mimalloc::MiMalloc;
#[cfg(not(any(feature = "allocator-mimalloc", feature = "allocator-jemalloc")))]
use std::alloc::System;
#[cfg(feature = "allocator-jemalloc")]
use tikv_jemallocator::Jemalloc;

#[cfg(feature = "allocator-mimalloc")]
#[global_allocator]
static ALLOCATOR: MiMalloc = MiMalloc;
#[cfg(feature = "allocator-jemalloc")]
#[global_allocator]
static ALLOCATOR: Jemalloc = Jemalloc;
#[cfg(not(any(feature = "allocator-mimalloc", feature = "allocator-jemalloc")))]
#[global_allocator]
static ALLOCATOR: System = System;

#[cfg(feature = "allocator-mimalloc")]
pub(super) const NAME: &str = "mimalloc";
#[cfg(feature = "allocator-jemalloc")]
pub(super) const NAME: &str = "jemalloc";
#[cfg(not(any(feature = "allocator-mimalloc", feature = "allocator-jemalloc")))]
pub(super) const NAME: &str = "system";
