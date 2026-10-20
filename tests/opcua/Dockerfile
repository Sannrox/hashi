FROM rust:1.99-bookworm@sha256:e12566d5abc17a0da741628782cdb504b68b8961f5bfcf677936bf3e8b32de29
WORKDIR /fixture
ENV CARGO_TARGET_DIR=/fixture/target
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY tests ./tests
COPY examples ./examples
RUN cargo test --locked --features opcua-simulator --test opcua_discovery --test opcua_subscription --no-run -j 2
CMD ["cargo", "test", "--locked", "--features", "opcua-simulator", "--test", "opcua_discovery", "--test", "opcua_subscription", "--", "--nocapture"]
