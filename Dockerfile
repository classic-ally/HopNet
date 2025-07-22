# syntax=docker/dockerfile:1
FROM nixos/nix:latest as builder

# Enable flakes and other modern nix features
RUN echo "experimental-features = nix-command flakes" >> /etc/nix/nix.conf

WORKDIR /app
COPY . .

# Use cache mounts with your existing shell.nix
RUN --mount=type=cache,target=/root/.cargo/registry \
    --mount=type=cache,target=/app/target \
    --mount=type=cache,target=/root/.npm \
    nix-shell --run "cargo build --release --bin hopnet" && \
    cp target/release/hopnet /hopnet

# Runtime stage
FROM nixos/nix:latest
RUN echo "experimental-features = nix-command flakes" >> /etc/nix/nix.conf
COPY --from=builder /hopnet /usr/local/bin/hopnet
EXPOSE 34633
CMD ["/usr/local/bin/hopnet"]