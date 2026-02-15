# syntax=docker/dockerfile:1
FROM nixos/nix:latest as builder

# Enable flakes and other modern nix features
RUN echo "experimental-features = nix-command flakes" >> /etc/nix/nix.conf

WORKDIR /app

# Copy shell.nix first and cache nix dependencies
COPY shell.nix ./
RUN nix-shell --run "echo 'Nix dependencies cached'"

# Copy dependency files for better cargo caching
COPY Cargo.toml Cargo.lock ./
COPY common/ common/
COPY frontend/package*.json frontend/
COPY frontend/vite.config.ts frontend/
COPY frontend/index.html frontend/
COPY frontend/tsconfig*.json frontend/
COPY frontend/svelte.config.js frontend/
COPY frontend/uno.config.ts frontend/

# Copy source code
COPY src/ src/
COPY frontend/src/ frontend/src/
COPY frontend/public/ frontend/public/
COPY frontend/dist/ frontend/dist/
COPY orchestrator/ orchestrator/

# Build with cached dependencies
RUN --mount=type=cache,target=/root/.cargo/registry \
    --mount=type=cache,target=/app/target \
    --mount=type=cache,target=/root/.npm \
    nix-shell --run "cargo build --release --bin hopnet" && \
    cp target/release/hopnet /hopnet

# Runtime stage
FROM nixos/nix:latest
RUN echo "experimental-features = nix-command flakes" >> /etc/nix/nix.conf
COPY --from=builder /hopnet /usr/local/bin/hopnet
EXPOSE 34632
ENV RUST_LOG=debug
CMD ["/usr/local/bin/hopnet"]