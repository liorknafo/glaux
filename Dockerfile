# glaux-server container image (Apache-2.0 binary only).
#
#   docker build -t glaux-server .
#   docker run --rm -p 4570:4570 glaux-server \
#     --listen 0.0.0.0:4570 \
#     --s3-endpoint http://host.docker.internal:4566 \
#     --glue-endpoint http://host.docker.internal:4566
#
# See examples/docker-compose.yml for the fakecloud pairing.

FROM rust:1.90-bookworm AS build
WORKDIR /src
COPY . .
RUN cargo build --release -p glaux-server --locked

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /src/target/release/glaux-server /usr/local/bin/glaux-server
EXPOSE 4570
ENV GLAUX_LISTEN=0.0.0.0:4570
HEALTHCHECK --interval=5s --timeout=3s --retries=12 \
    CMD curl -fsS http://127.0.0.1:4570/health || exit 1
ENTRYPOINT ["glaux-server"]
