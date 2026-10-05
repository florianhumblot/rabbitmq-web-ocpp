# Build context: examples/polyglot-demo (the dashboard page and the registry
# script are shared with the other implementations).
FROM rust:1-bookworm AS build
WORKDIR /src/rust
# Build the dependencies first so they are cached across source changes.
COPY rust/Cargo.toml rust/Cargo.lock ./
RUN mkdir src && echo 'fn main() {}' > src/main.rs && cargo build --release && rm -rf src
COPY dashboard /src/dashboard
COPY valkey/registry.lua /src/valkey/registry.lua
COPY rust/src src
RUN touch src/main.rs && cargo build --release

FROM debian:bookworm-slim
COPY --from=build /src/rust/target/release/csms-rust /usr/local/bin/csms-rust
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/csms-rust"]
