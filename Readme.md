
# Liquidation Bot

## App Commands

`cargo build` - build in debug

`cargo build --release` - build in release

`cargo run` - run app in debug

`cargo run --release` - run app in release

`RUST_LOG=debug cargo run -- start` - run liquidation bot with debug level logger

`RUST_LOG=debug cargo run --bin bot -- start` - run liquidation bot with debug level logger

`RUST_LOG=debug cargo run --release --bin bot -- start` - run liquidation bot in release mode with debug level logger

`cargo check` - fast check

`cargo fmt` - format code

`cargo clippy` - link with Clippy

`cargo clippy -- -D warnings` - link with Clippy, treat warnings as errors

`cargo test` - run tests

`cargo test -- --nocapture` - run tests with output

`cargo test my_test_name` - run test my_test_name

`cargo add crate` - add crate

`cargo update` - update all dependencies

`cargo clean` - removes build artifacts

`cargo tree` - show dependency tree

`cargo new my_app` - creates a new binary project

`cargo new --lib my_lib` - creates a new library project

`cargo build --target x86_64-unknown-linux-gnu` - build for a different platform




**Future Improvements**



1. check debugs
2. we need some kind api to check status, to see cache and etc