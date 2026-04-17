# Custom Airflow image with dbt-core, dbt-postgres, and astronomer-cosmos
# pre-baked at build time (the approach Airflow officially recommends).
#
# Why not _PIP_ADDITIONAL_REQUIREMENTS?
#   That env var re-runs pip on every container restart and the slim base image
#   lacks the C build tools required to compile some dbt dependencies.
#
# Why psycopg2-binary instead of psycopg2?
#   The Airflow base image ships the PostgreSQL APT repo (pgdg) which places
#   pg_config outside the standard PATH, breaking psycopg2's source build.
#   psycopg2-binary is a self-contained wheel with no compilation step.
#   dbt-postgres is installed --no-deps because its only two dependencies
#   (dbt-core and psycopg2) are already satisfied above.
#
# Why gcc?
#   logbook (a dbt-core dependency) lacks a pre-built wheel for Python 3.12,
#   so it must be compiled from source. gcc + the Python headers that come
#   with the python:3.12-slim base image are sufficient.

FROM apache/airflow:2.9.1

# ── Install gcc as root (needed to compile logbook for Python 3.12) ──────────
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends gcc \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# ── Install Python packages as the airflow user ───────────────────────────────
USER airflow
RUN pip install --no-cache-dir \
        "psycopg2-binary>=2.9.6" \
        "dbt-core==1.8.0" \
        "astronomer-cosmos==1.4.3" \
    && pip install --no-cache-dir --no-deps "dbt-postgres==1.8.0"
