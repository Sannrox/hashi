FROM rust:1.99-bookworm@sha256:e12566d5abc17a0da741628782cdb504b68b8961f5bfcf677936bf3e8b32de29
WORKDIR /fixture
ENV CARGO_TARGET_DIR=/fixture/target
RUN apt-get update \
    && apt-get install -y --no-install-recommends python3 python3-pip \
    && rm -rf /var/lib/apt/lists/*
COPY tests/opcua/foreign-requirements.txt ./foreign-requirements.txt
RUN pip3 install --break-system-packages --no-cache-dir -r foreign-requirements.txt
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY tests ./tests
COPY examples ./examples
RUN cargo test --locked --features opcua-simulator --test opcua_foreign --no-run -j 2
ENV HASHI_OPCUA_FOREIGN=1
CMD ["cargo", "test", "--locked", "--features", "opcua-simulator", "--test", "opcua_foreign", "--", "--ignored", "--nocapture", "--test-threads=1"]
