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
# Why pandas is installed --no-deps in its own step, and SQLAlchemy is NOT pinned?
#   Both already ride in transitively via mlflow's own dependencies. Adding
#   them as top-level pinned requirements in the SAME pip install as
#   astronomer-cosmos/mlflow/scikit-learn/openai forces pip's resolver to
#   jointly re-solve the entire dependency graph (including apache-airflow's,
#   since cosmos depends on it), which triggers catastrophic backtracking
#   ("pip is looking at multiple versions of flask-appbuilder... this could
#   take a while" — observed to run 60+ minutes without finishing). Installing
#   pandas --no-deps in its own step verifies/upgrades the already-satisfied
#   version without re-resolving anything else.
#   SQLAlchemy is deliberately left un-pinned: Airflow 2.9.1's own ORM models
#   (e.g. TaskInstance) use SQLAlchemy 1.4-style declarative annotations that
#   are NOT compatible with SQLAlchemy 2.0's Mapped[] typing requirements —
#   forcing an upgrade to 2.0 crashloops the webserver/scheduler with
#   `sqlalchemy.orm.exc.MappedAnnotationError` (confirmed empirically). The
#   transitively-installed 1.4.x is what Airflow itself requires, and it's
#   sufficient for the ingest_olist task (engine.begin()/text()/df.to_sql()
#   are all supported since SQLAlchemy 1.4).
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
        "mlflow>=2.13.0,<3.0.0" \
        "scikit-learn>=1.4.0,<2.0.0" \
        "openai>=1.30.0,<2.0.0" \
    && pip install --no-cache-dir --no-deps "dbt-postgres==1.8.0"

# Kept as its own layer (not chained with `&&` above) so this line is the
# only one that invalidates on changes to the pandas pin — the heavy install
# above stays cache-identical to prior builds.
RUN pip install --no-cache-dir --no-deps "pandas>=2.0.0,<3.0.0"
