"""
mlflow_training DAG
════════════════════
Reads from mart_customer_orders, trains a RandomForestRegressor to predict
customer lifetime_value, and logs params/metrics/model artifact to MLflow.

Requires the mlops profile:  docker-compose --profile mlops up -d
MLflow UI → http://localhost:5000
"""
from __future__ import annotations

import os
from datetime import datetime

from airflow.decorators import dag, task


@dag(
    dag_id="mlflow_training",
    description="Train a lifetime-value predictor and track with MLflow.",
    start_date=datetime(2024, 1, 1),
    schedule="@weekly",
    catchup=False,
    tags=["mlops", "mlflow", "training"],
)
def mlflow_training() -> None:

    @task
    def train_and_log() -> dict:
        import mlflow
        import mlflow.sklearn
        import pandas as pd
        import sqlalchemy
        from sklearn.ensemble import RandomForestRegressor
        from sklearn.metrics import mean_absolute_error, r2_score
        from sklearn.model_selection import train_test_split

        # ── Read from the warehouse mart ──────────────────────────────────────
        db_user = os.getenv("WAREHOUSE_DB_USER", "warehouse")
        db_password = os.getenv("WAREHOUSE_DB_PASSWORD", "warehouse")
        db_name = os.getenv("WAREHOUSE_DB_NAME", "warehouse")
        engine = sqlalchemy.create_engine(
            f"postgresql+psycopg2://{db_user}:{db_password}@postgres_warehouse:5432/{db_name}"
        )
        df = pd.read_sql(
            "SELECT total_orders, avg_order_value, completed_orders, cancelled_orders, "
            "lifetime_value FROM public_marts.mart_customer_orders",
            engine,
        )

        features = ["total_orders", "avg_order_value", "completed_orders", "cancelled_orders"]
        target = "lifetime_value"

        X = df[features]
        y = df[target]

        # With seed data (~10 rows), test_size=1 gives at least one test sample.
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=max(1, int(len(df) * 0.2)), random_state=42
        )

        # ── Train ─────────────────────────────────────────────────────────────
        tracking_uri = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow-server:5000")
        mlflow.set_tracking_uri(tracking_uri)
        mlflow.set_experiment("lifetime_value_prediction")

        params = {"n_estimators": 100, "max_depth": 5, "random_state": 42}

        with mlflow.start_run() as run:
            model = RandomForestRegressor(**params)
            model.fit(X_train, y_train)

            y_pred = model.predict(X_test)
            metrics = {
                "mae": mean_absolute_error(y_test, y_pred),
                "r2":  r2_score(y_test, y_pred),
                "train_rows": len(X_train),
                "test_rows":  len(X_test),
            }

            mlflow.log_params(params)
            mlflow.log_params({"features": ",".join(features)})
            mlflow.log_metrics(metrics)
            mlflow.sklearn.log_model(model, "lifetime_value_model")

            run_id = run.info.run_id

        return {"run_id": run_id, **metrics}

    train_and_log()


mlflow_training()
