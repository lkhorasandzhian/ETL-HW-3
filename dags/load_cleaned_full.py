from __future__ import annotations
from datetime import datetime
import pandas as pd
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook


TARGET_CONN_ID = "target_pg"
CSV_PATH = "/opt/airflow/data/cleaned.csv"
TARGET_TABLE = "cleaned"


DDL_SQL = f"""
CREATE TABLE IF NOT EXISTS public.{TARGET_TABLE} (
  id        text PRIMARY KEY,
  room_id   text,
  noted_date date,
  temp      integer,
  in_out    text
);
"""


@dag(
    dag_id="load_cleaned_full",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["etl", "full", "manual"]
)
def load_cleaned_full():
    @task
    def create_table():
        hook = PostgresHook(postgres_conn_id=TARGET_CONN_ID)
        hook.run(DDL_SQL)

    @task
    def truncate_target():
        hook = PostgresHook(postgres_conn_id=TARGET_CONN_ID)
        hook.run(f"TRUNCATE TABLE public.{TARGET_TABLE};")

    @task
    def load_all_from_csv():
        df = pd.read_csv(CSV_PATH)

        df = df.rename(columns={"room_id/id": "room_id", "out/in": "in_out"})

        df["noted_date"] = pd.to_datetime(df["noted_date"], errors="coerce").dt.date
        df["temp"] = pd.to_numeric(df["temp"], errors="coerce").astype("Int64")

        df = df.drop_duplicates(subset=["id"])

        hook = PostgresHook(postgres_conn_id=TARGET_CONN_ID)

        rows = df[["id", "room_id", "noted_date", "temp", "in_out"]].values.tolist()
        hook.insert_rows(
            table=f"public.{TARGET_TABLE}",
            rows=rows,
            target_fields=["id", "room_id", "noted_date", "temp", "in_out"],
            replace=False
        )

    create_table() >> truncate_target() >> load_all_from_csv()


load_cleaned_full()
