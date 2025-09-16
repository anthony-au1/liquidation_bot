
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


## Stats 

`curl http://localhost:3000/full_state | jq` - full stats

`curl http://localhost:3000/users | jq` - users

`curl http://localhost:3000/decimals | jq` - decimals

`curl http://localhost:3000/reserve | jq` - reserve

`curl http://localhost:3000/collateral | jq` - collateral

`curl http://localhost:3000/collateral_matrix | jq` - collateral matrix

`curl http://localhost:3000/borrowed | jq` - borrowed

`curl http://localhost:3000/borrowed_matrix | jq` - borrowed matrix

`curl http://localhost:3000/liquidity | jq` - liquidity

`curl http://localhost:3000/liquidity_index | jq` - liquidity index

`curl http://localhost:3000/variable_borrow | jq` - variable borrow

`curl http://localhost:3000/variable_borrow_index | jq` - variable borrow index

`curl http://localhost:3000/liquidation_threshold | jq` - liquidation threshold

`curl http://localhost:3000/prices | jq` - prices

`curl http://localhost:3000/health_factors | jq` - health factors



**Future Improvements**



1. get init prices
2. answer updated is not firing