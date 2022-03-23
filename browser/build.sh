#!/bin/bash -e
RUSTFLAGS='-C target-feature=+atomics,+bulk-memory,+mutable-globals -C link-args=--no-check-features' \
  cargo build --target wasm32-unknown-unknown --release -Z build-std=std,panic_abort

wasm-bindgen \
  ./target/wasm32-unknown-unknown/release/sample.wasm \
  --out-dir . \
  --target web \
  --weak-refs
