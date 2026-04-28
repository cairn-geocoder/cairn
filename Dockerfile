# syntax=docker/dockerfile:1.6
#
# Multi-stage build that produces a small Alpine image containing:
#   - statically linked cairn-build and cairn-serve binaries (musl)
#   - a pre-built Liechtenstein bundle baked at /bundle
#
# Override BUNDLE_ID, OSM_URL, WOF_URL at build time to bake a different
# region into the image.

ARG RUST_TAG=alpine
ARG ALPINE_VERSION=3.20

############################################################
# Builder: compile cairn-build + cairn-serve against musl libc.
# Default `rust:alpine` tracks the latest stable Rust on Alpine.
############################################################
FROM rust:${RUST_TAG} AS builder
RUN apk add --no-cache \
        musl-dev \
        pkgconfig \
        openssl-dev \
        openssl-libs-static \
        clang \
        protoc

WORKDIR /src
# Copy only manifests first so the dependency layer caches when source
# changes but Cargo.toml does not.
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY bins/cairn-build/Cargo.toml bins/cairn-build/Cargo.toml
COPY bins/cairn-serve/Cargo.toml bins/cairn-serve/Cargo.toml
COPY crates/ crates/
# Now bring in the actual binary sources.
COPY bins/cairn-build/src bins/cairn-build/src
COPY bins/cairn-serve/src bins/cairn-serve/src

RUN cargo build --release -p cairn-build -p cairn-serve

############################################################
# Bundler: download source data, run cairn-build, emit /bundle.
############################################################
FROM alpine:${ALPINE_VERSION} AS bundler
ARG BUNDLE_ID="li-cluster"
ARG OSM_URL="https://download.geofabrik.de/europe/liechtenstein-latest.osm.pbf"
ARG WOF_URL="https://data.geocode.earth/wof/dist/sqlite/whosonfirst-data-admin-li-latest.db.bz2"
RUN apk add --no-cache curl bzip2 ca-certificates
WORKDIR /data
RUN curl -fsSL -o source.osm.pbf "$OSM_URL"
RUN curl -fsSL -o wof.db.bz2 "$WOF_URL" && bunzip2 wof.db.bz2

COPY --from=builder /src/target/release/cairn-build /usr/local/bin/cairn-build
RUN /usr/local/bin/cairn-build build \
        --osm /data/source.osm.pbf \
        --wof /data/wof.db \
        --out /bundle \
        --bundle-id "$BUNDLE_ID"

############################################################
# Final image: minimal runtime with binaries + baked bundle.
# Includes cairn-build, curl, bzip2 so the same image can run a
# bundle-build Job in Kubernetes (downloads sources + emits a fresh
# bundle to a mounted PVC).
############################################################
FROM alpine:${ALPINE_VERSION}
RUN apk add --no-cache ca-certificates curl bzip2 && \
    addgroup -S cairn && adduser -S -G cairn -h /home/cairn cairn

COPY --from=builder /src/target/release/cairn-serve /usr/local/bin/cairn-serve
COPY --from=builder /src/target/release/cairn-build /usr/local/bin/cairn-build
COPY --from=bundler /bundle /bundle
RUN chown -R cairn:cairn /bundle

USER cairn
WORKDIR /home/cairn

EXPOSE 8080
ENV RUST_LOG=info
ENTRYPOINT ["/usr/local/bin/cairn-serve"]
CMD ["--bundle", "/bundle", "--bind", "0.0.0.0:8080"]
