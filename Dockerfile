# Offline image: one static binary on an empty base. The build image is pinned by
# digest; the binary makes no network calls beyond configured sources and sinks.
FROM rust:1.99-alpine@sha256:3ea887915f261f273d6a24efdfdb92665dee12199923e57870453644fe4ae5c9 AS build
RUN apk add --no-cache musl-dev
WORKDIR /src
COPY . .
RUN cargo build --release --locked --bin hashi \
    && ! ldd target/release/hashi 2>/dev/null \
    && cp target/release/hashi /hashi

FROM scratch
COPY --from=build /hashi /hashi
USER 65534:65534
ENTRYPOINT ["/hashi"]
