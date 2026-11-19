"""
Extract & Load: Neon (PostgreSQL, schemat src) -> BigQuery (dataset landing)

Symulacja CDC opartego na znaczniku czasu (kolumna updated_at).
Skrypt ładuje wszystkie tabele bazy źródłowej w kolejności wynikającej
z kluczy obcych.

Tryby ładowania (zmienna środowiskowa LOAD_MODE):
  full_refresh -> tabela w landing jest czyszczona i ładowane są wszystkie rekordy
  incremental  -> ładowane są tylko rekordy z updated_at większym niż
                  maksymalny updated_at już obecny w landing (append-only,
                  zmieniony rekord dopisywany jest jako kolejna wersja)

Każdy załadowany wiersz dostaje kolumny techniczne:
  _loaded_at  -> moment ładowania (UTC)
  _load_mode  -> tryb ładowania, w którym wiersz trafił do landing
"""

import os
from datetime import datetime, timezone

import pandas as pd
import psycopg2
from google.cloud import bigquery

# ============================================================
# KONFIGURACJA
# ============================================================

NEON_CONNECTION_STRING = os.environ["NEON_CONNECTION_STRING"]
BQ_PROJECT_ID = os.environ["BQ_PROJECT_ID"]
BQ_DATASET = os.environ["BQ_DATASET"]

# Tryb można wybrać przy ręcznym uruchomieniu workflow (domyślnie incremental)
LOAD_MODE = os.environ.get("LOAD_MODE", "incremental")

SOURCE_SCHEMA = "src"
INCREMENTAL_COLUMN = "updated_at"

# Kolejność ładowania zgodna z relacjami (klucze obce):
# 1. słowniki -> 2. tabele zależne od słowników -> 3. orders -> 4. order_items
TABLES_IN_LOAD_ORDER = [
    "regions",
    "categories",
    "stores",
    "products",
    "customers",
    "orders",
    "order_items",
]

# ============================================================
# LOGIKA
# ============================================================


def get_last_loaded_timestamp(bq_client: bigquery.Client, table_ref: str):
    """Zwraca MAX(updated_at) z tabeli w landing albo None, jeśli tabela nie istnieje."""
    query = f"SELECT MAX({INCREMENTAL_COLUMN}) AS max_ts FROM `{table_ref}`"
    try:
        row = next(iter(bq_client.query(query).result()))
        return row.max_ts
    except Exception:
        return None


def extract_from_neon(conn, table: str, mode: str, last_ts=None) -> pd.DataFrame:
    """Wyciąga rekordy z tabeli src.<table> (wszystkie lub tylko zmienione)."""
    source = f"{SOURCE_SCHEMA}.{table}"
    if mode == "incremental" and last_ts is not None:
        query = f"SELECT * FROM {source} WHERE {INCREMENTAL_COLUMN} > %s ORDER BY {INCREMENTAL_COLUMN}"
        return pd.read_sql(query, conn, params=(last_ts,))
    return pd.read_sql(f"SELECT * FROM {source} ORDER BY {INCREMENTAL_COLUMN}", conn)


def load_to_bigquery(bq_client: bigquery.Client, df: pd.DataFrame, table_ref: str, mode: str):
    """full_refresh -> WRITE_TRUNCATE, incremental -> WRITE_APPEND."""
    write_disposition = (
        bigquery.WriteDisposition.WRITE_TRUNCATE
        if mode == "full_refresh"
        else bigquery.WriteDisposition.WRITE_APPEND
    )
    job_config = bigquery.LoadJobConfig(write_disposition=write_disposition)
    job = bq_client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()
    return job


def main():
    if LOAD_MODE not in ("full_refresh", "incremental"):
        raise ValueError(f"Nieznany LOAD_MODE: {LOAD_MODE}")

    bq_client = bigquery.Client(project=BQ_PROJECT_ID)
    loaded_at = datetime.now(timezone.utc)
    print(f"Tryb ładowania: {LOAD_MODE} | _loaded_at = {loaded_at.isoformat()}")

    conn = psycopg2.connect(NEON_CONNECTION_STRING)
    try:
        for step, table in enumerate(TABLES_IN_LOAD_ORDER, start=1):
            table_ref = f"{BQ_PROJECT_ID}.{BQ_DATASET}.{table}"
            print(f"\n[{step}/{len(TABLES_IN_LOAD_ORDER)}] {SOURCE_SCHEMA}.{table} -> {table_ref}")

            last_ts = None
            if LOAD_MODE == "incremental":
                last_ts = get_last_loaded_timestamp(bq_client, table_ref)
                print(f"  Ostatni załadowany {INCREMENTAL_COLUMN}: {last_ts}")

            df = extract_from_neon(conn, table, LOAD_MODE, last_ts)
            print(f"  Wyciągnięto {len(df)} wierszy z Neon")

            if df.empty:
                print("  Brak nowych/zmienionych rekordów -- pomijam.")
                continue

            df["_loaded_at"] = loaded_at
            df["_load_mode"] = LOAD_MODE

            job = load_to_bigquery(bq_client, df, table_ref, LOAD_MODE)
            print(f"  Załadowano {job.output_rows} wierszy (tryb: {LOAD_MODE})")
    finally:
        conn.close()

    print("\nŁadowanie zakończone.")


if __name__ == "__main__":
    main()
