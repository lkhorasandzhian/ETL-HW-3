from __future__ import annotations
from datetime import datetime, timedelta
import pandas as pd
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook


TARGET_CONN_ID = "target_pg"
CSV_PATH = "/opt/airflow/data/cleaned.csv"
TARGET_TABLE = "cleaned"
N_DAYS = 5


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
    dag_id="load_cleaned_last_days",
    start_date=datetime(2026, 1, 1),
    schedule="0 6 * * *",
    catchup=False,
    tags=["etl", "last_days", "scheduled"]
)
def load_cleaned_last_days():
    @task
    def create_table():
        hook = PostgresHook(postgres_conn_id=TARGET_CONN_ID)
        hook.run(DDL_SQL)

    @task
    def compute_start_date() -> str:
        start_date = (datetime.now().date() - timedelta(days=N_DAYS))
        return start_date.isoformat()

    @task
    def delete_window(start_date: str):
        hook = PostgresHook(postgres_conn_id=TARGET_CONN_ID)
        hook.run(
            f"DELETE FROM public.{TARGET_TABLE} WHERE noted_date >= %s::date;",
            parameters=(start_date)
        )

    @task
    def load_window_from_csv(start_date: str):
        df = pd.read_csv(CSV_PATH)
        df = df.rename(columns={"room_id/id": "room_id", "out/in": "in_out"})

        df["noted_date"] = pd.to_datetime(df["noted_date"], errors="coerce").dt.date
        df["temp"] = pd.to_numeric(df["temp"], errors="coerce").astype("Int64")

        start = datetime.fromisoformat(start_date).date()
        df = df[df["noted_date"] >= start]

        df = df.drop_duplicates(subset=["id"])

        hook = PostgresHook(postgres_conn_id=TARGET_CONN_ID)
        rows = df[["id", "room_id", "noted_date", "temp", "in_out"]].values.tolist()

        if rows:
            hook.insert_rows(
                table=f"public.{TARGET_TABLE}",
                rows=rows,
                target_fields=["id", "room_id", "noted_date", "temp", "in_out"],
                replace=False
            )

    start_date = compute_start_date()
    create_table() >> start_date >> delete_window(start_date) >> load_window_from_csv(start_date)


load_cleaned_last_days()
