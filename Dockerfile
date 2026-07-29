FROM rust:1.95.0-bookworm AS builder

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates libssl-dev pkg-config \
    && rm -rf /var/lib/apt/lists/*

COPY . .

# sqlx compile-time checks need DATABASE_URL when offline metadata is not present.
ARG DATABASE_URL
ENV DATABASE_URL=${DATABASE_URL}

RUN cargo build --release --bin exchange-shared

FROM debian:bookworm-slim AS runtime

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates libssl3 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/exchange-shared /app/exchange-shared

ENV PORT=3000

EXPOSE 3000

CMD ["/app/exchange-shared"]
