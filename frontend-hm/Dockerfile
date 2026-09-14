# Frontend Dockerfile
# Supports two targets:
#   --target dev   — install deps only, run `pnpm dev` at container start
#   --target prod  — full build baked in, run `pnpm start` at container start (default if no --target is specified)

ARG PNPM_STORE_PATH=/root/.local/share/pnpm/store
ARG NPM_REGISTRY

# ── Base: shared setup ────────────────────────────────────────────────────────
FROM node:22-alpine AS pnpm-runtime
ARG PNPM_STORE_PATH
ARG NPM_REGISTRY
ENV COREPACK_HOME=/opt/corepack
# Configure corepack registry before installing pnpm so the download itself
# succeeds in restricted networks (COREPACK_NPM_REGISTRY controls where
# corepack fetches package managers from). Keep the downloaded package manager
# outside root's home so the non-root runtime user can use it without egress.
RUN if [ -n "${NPM_REGISTRY}" ]; then \
      export COREPACK_NPM_REGISTRY="${NPM_REGISTRY}"; \
    fi && \
    corepack enable && \
    corepack install -g pnpm@10.26.2 && \
    chmod -R a+rX "${COREPACK_HOME}"
ENV COREPACK_ENABLE_NETWORK=0
RUN pnpm config set store-dir ${PNPM_STORE_PATH}
# Optionally override npm registry for restricted networks (e.g. NPM_REGISTRY=https://registry.npmmirror.com)
RUN if [ -n "${NPM_REGISTRY}" ]; then pnpm config set registry "${NPM_REGISTRY}"; fi

# ── Base: shared source setup ───────────────────────────────────────────────
FROM pnpm-runtime AS base
WORKDIR /app
COPY frontend ./frontend

# ── Dev: install only, CMD is overridden by docker-compose ───────────────────
FROM base AS dev
RUN cd /app/frontend && pnpm install --frozen-lockfile
EXPOSE 3000

# ── Builder: install + compile Next.js ───────────────────────────────────────
FROM base AS builder
RUN cd /app/frontend && pnpm install --frozen-lockfile
# Skip env validation — runtime vars are injected by nginx/container
# App version stamp for the About page. Nightly CI passes
# APP_VERSION=<base>-nightly.<date>-<sha>; release/local builds leave it empty
# and the frontend falls back to package.json's version.
ARG APP_VERSION=""
ENV NEXT_PUBLIC_APP_VERSION=$APP_VERSION
RUN cd /app/frontend && SKIP_ENV_VALIDATION=1 pnpm build

# ── Prod: minimal runtime with pre-built output ───────────────────────────────
FROM pnpm-runtime AS prod
WORKDIR /app
COPY --from=builder /app/frontend ./frontend
EXPOSE 3000
CMD ["sh", "-c", "cd /app/frontend && pnpm start"]
