#[cfg(any(not(target_arch = "wasm32"), feature = "wasm-threads"))]
pub fn parallel_enabled() -> bool {
    true
}

#[cfg(not(any(not(target_arch = "wasm32"), feature = "wasm-threads")))]
pub fn parallel_enabled() -> bool {
    false
}

#[cfg(any(not(target_arch = "wasm32"), feature = "wasm-threads"))]
pub fn parallel_thread_count() -> usize {
    rayon::current_num_threads()
}

#[cfg(not(any(not(target_arch = "wasm32"), feature = "wasm-threads")))]
pub fn parallel_thread_count() -> usize {
    1
}
