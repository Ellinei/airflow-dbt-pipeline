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
# Why --constraint against Airflow's own constraints file, and requirements.txt?
#   Every extra package below is exact-pinned in requirements.txt (see that
#   file for regeneration instructions) instead of the version ranges this
#   image used to carry. Installing pandas/astronomer-cosmos/mlflow/
#   scikit-learn/openai together in ONE pip install used to trigger
#   catastrophic resolver backtracking when their versions were open-ended
#   ranges ("pip is looking at multiple versions of flask-appbuilder... this
#   could take a while" — observed to run 60+ minutes without finishing).
#   With every version exact-pinned there's no range for pip to search, so a
#   single joint resolve is fast and safe — verified empirically (~2 min,
#   no backtracking) with the --constraint flag below.
#   The --constraint URL pins apache-airflow's OWN dependency tree (including
#   SQLAlchemy==1.4.52 — see below) without adding new requirements to
#   resolve, which is what keeps this fast rather than reintroducing
#   backtracking.
#
# Why pandas and dbt-postgres are still installed --no-deps in their own steps?
#   Their dependencies are already satisfied by the packages above (pandas
#   transitively via mlflow; dbt-postgres needs only dbt-core + psycopg2).
#   Folding them into the same joint resolve as the ranged packages used to
#   was part of what caused the old backtracking; keeping them isolated
#   avoids pulling their own (unpinned) dependency trees into any resolve.
#
# Why SQLAlchemy is not installed directly?
#   Airflow 2.9.1's own ORM models (e.g. TaskInstance) use SQLAlchemy 1.4-style
#   declarative annotations that are NOT compatible with SQLAlchemy 2.0's
#   Mapped[] typing requirements — forcing an upgrade to 2.0 crashloops the
#   webserver/scheduler with `sqlalchemy.orm.exc.MappedAnnotationError`
#   (confirmed empirically). The --constraint file pins it to 1.4.52
#   transitively, which is what Airflow itself requires and is sufficient for
#   the ingest_olist task (engine.begin()/text()/df.to_sql() are all
#   supported since SQLAlchemy 1.4).
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

# Airflow's own constraints file for this exact release/Python combo — pins
# apache-airflow's dependency tree (incl. SQLAlchemy) without adding new
# requirements to resolve. See the block comment above for why this matters.
ARG AIRFLOW_CONSTRAINTS_URL="https://raw.githubusercontent.com/apache/airflow/constraints-2.9.1/constraints-3.12.txt"

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir --constraint "${AIRFLOW_CONSTRAINTS_URL}" \
        -r /tmp/requirements.txt \
    && pip install --no-cache-dir --no-deps "dbt-postgres==1.8.0"

# Kept as its own layer (not chained with `&&` above) so this line is the
# only one that invalidates on changes to the pandas pin — the heavy install
# above stays cache-identical to prior builds.
RUN pip install --no-cache-dir --no-deps "pandas==2.1.4"
