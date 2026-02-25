# syntax=docker/dockerfile:1
FROM nixos/nix:latest AS builder

RUN echo "experimental-features = nix-command flakes" >> /etc/nix/nix.conf

WORKDIR /app
COPY . .

RUN nix build .#default

# Export the runtime closure (binary + all nix store deps)
RUN mkdir -p /export/nix/store /export/usr/local/bin && \
    nix-store -qR result | xargs -I{} cp -a {} /export/nix/store/ && \
    cp -L result/bin/hopnet /export/usr/local/bin/hopnet

FROM busybox:latest
COPY --from=builder /export/ /
EXPOSE 34632
ENV RUST_LOG=warn,hopnet=debug
ENTRYPOINT ["/usr/local/bin/hopnet"]
