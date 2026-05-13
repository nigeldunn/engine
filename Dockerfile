# syntax=docker/dockerfile:1.7
#
# Multi-stage build for `orchestrator-app`. Designed for ECS Fargate
# ARM64; build under emulation from any host with `docker buildx build
# --platform linux/arm64 .` or build natively on an ARM64 host.
#
# Why glibc + distroless (not musl + alpine):
#   - sqlx (postgres) and reqwest (rustls) link against glibc-flavored
#     C shims; musl builds have working but sluggish TLS on Aurora
#     resume paths.
#   - distroless/cc-debian12 ships glibc, libgcc1, ca-certificates, and a
#     nonroot user — no shell, no package manager. Smallest viable
#     runtime image for this binary.

# Pin to a specific Rust minor so the same commit produces the same
# binary across rebuilds (Codex Stage-E review: a floating `1-bookworm`
# tag would silently move when Debian or rustup ships a new minor).
# Bump deliberately when adopting new compiler features; CI / clippy
# warnings will surface any required follow-up.
FROM rust:1.95-bookworm AS build
WORKDIR /src

# Minimal build deps. `ca-certificates` lets cargo fetch crates over
# HTTPS during build; `pkg-config` is harmless and pulled in transitively
# by a couple of crates even when we use rustls (no OpenSSL needed).
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates pkg-config \
 && rm -rf /var/lib/apt/lists/*

# Copy the workspace. `.dockerignore` strips local state, docs, and
# target/. A future optimization is cargo-chef for dep-layer caching;
# for personal-use builds the simple copy-and-build path is fine.
COPY . .

# `--locked` keeps Cargo.lock authoritative so the image is reproducible
# from a given commit. Build only the app binary — workspace tests and
# unrelated bins stay out of the runtime artifact.
RUN cargo build --release --locked --bin orchestrator-app

# ---- Runtime ----

# Floating tag is acceptable for personal-use: Debian security patches
# arrive automatically on rebuild, and the binary's behavior is pinned
# by Cargo.lock. For a production deployment that requires bit-exact
# reproducibility, replace with `@sha256:...` digest pinning.
FROM gcr.io/distroless/cc-debian12:nonroot

# Single statically-located binary so ECS task definitions and operators
# don't have to discover paths. `/usr/local/bin` is on $PATH even though
# distroless has no shell — execve resolution still uses it.
COPY --from=build /src/target/release/orchestrator-app /usr/local/bin/orchestrator-app

# Default config path. ECS deployments bind-mount the actual config (or
# render it from Secrets Manager values) at `/etc/orchestrator.toml`. The
# runbook covers per-field overrides via `ORCH_*` env vars; the config
# file itself does not need to contain secrets in production.
ENTRYPOINT ["/usr/local/bin/orchestrator-app"]
CMD ["--config", "/etc/orchestrator.toml"]
