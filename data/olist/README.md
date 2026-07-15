# Olist raw data

Download the Kaggle dataset **"Brazilian E-Commerce Public Dataset by Olist"**
(`olistbr/brazilian-ecommerce`) and place these 9 files directly in this
folder — no subfolders:

- `olist_customers_dataset.csv`
- `olist_orders_dataset.csv`
- `olist_order_items_dataset.csv`
- `olist_order_payments_dataset.csv`
- `olist_order_reviews_dataset.csv`
- `olist_products_dataset.csv`
- `olist_sellers_dataset.csv`
- `olist_geolocation_dataset.csv`
- `product_category_name_translation.csv`

Total size is roughly 120 MB. These CSVs are git-ignored (`data/olist/*.csv`
in `.gitignore`) — never commit them.

Once in place, the `ingest_olist` Airflow task (`dags/dbt_pipeline_dag.py`)
loads them into `raw.olist_*` Postgres tables on the next DAG run.
