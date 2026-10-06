# The read-side Postgres gets four extra extensions beyond pgvector/pgcrypto
# (which the write side still gets from the plain pgvector/pgvector:pg16
# image): pg_duckdb, timescaledb, pg_partman, and pg_search. All four are
# aimed at the query side specifically -- vectorized analytical scans,
# continuous aggregates, keeping partition/index sizes bounded for cheap
# cursor pagination, and BM25 full-text search for ModeSearch filters --
# so they're only added here, not on the write side.
#
# No single published image ships all of these together:
#   - pg_duckdb has no prebuilt .deb/.rpm releases at all (checked its
#     GitHub releases directly -- source only) and building it from scratch
#     means replicating its Rust + vendored-DuckDB build toolchain. The
#     official pgduckdb/pgduckdb image already has it compiled, so that's
#     the base here instead of building it ourselves.
#   - timescaledb and pg_partman are ordinary, lightweight Debian packages
#     (PGDG's own postgresql-16-partman/postgresql-16-pgvector, and
#     TimescaleDB's own apt repo), so they're layered on top via apt rather
#     than trying to move pg_duckdb's compiled .so onto a different base
#     image and OS (pgduckdb's image is Debian bookworm; risking a glibc/ABI
#     mismatch by copying its binary elsewhere wasn't worth it when apt
#     already solves the other two cleanly).
#   - pg_search (ParadeDB) ships prebuilt .deb releases directly on GitHub
#     for Debian bookworm, both amd64 and arm64 (confirmed against the
#     actual release assets, not guessed: `gh release view v0.25.11 --repo
#     paradedb/paradedb`) -- no separate apt repo to add, just a direct
#     download+install, same shape as the manual .deb step below. It
#     requires pgvector to already be installed (already true here) and
#     `shared_preload_libraries=pg_search` (set in docker-compose.yml,
#     alongside the pg_duckdb/timescaledb entries already there for the
#     same reason).
FROM pgduckdb/pgduckdb:16-main

USER root

ARG PG_SEARCH_VERSION=0.25.11

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl gnupg ca-certificates \
    && apt-get install -y --no-install-recommends postgresql-16-partman postgresql-16-pgvector \
    && curl -fsSL https://packagecloud.io/timescale/timescaledb/gpgkey | gpg --dearmor -o /usr/share/keyrings/timescaledb.gpg \
    && echo "deb [signed-by=/usr/share/keyrings/timescaledb.gpg] https://packagecloud.io/timescale/timescaledb/debian/ bookworm main" > /etc/apt/sources.list.d/timescaledb.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends timescaledb-2-postgresql-16 \
    && arch="$(dpkg --print-architecture)" \
    && curl -fsSL -o /tmp/pg_search.deb \
        "https://github.com/paradedb/paradedb/releases/download/v${PG_SEARCH_VERSION}/postgresql-16-pg-search_${PG_SEARCH_VERSION}-1PARADEDB-bookworm_${arch}.deb" \
    && apt-get install -y --no-install-recommends /tmp/pg_search.deb \
    && rm -f /tmp/pg_search.deb \
    && apt-get purge -y curl gnupg \
    && apt-get autoremove -y \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER postgres
