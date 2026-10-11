"""Discover financial-report announcements from HNX and SSC into PostgreSQL."""

from __future__ import annotations

import json
import os
from datetime import timedelta

import pendulum
from airflow.sdk import dag, get_current_context, task
from airflow.sdk.exceptions import AirflowSkipException
from airflow.providers.postgres.hooks.postgres import PostgresHook

from disclosure_sources import collect_disclosures

POSTGRES_CONN_ID = os.getenv("COLLECTOR_POSTGRES_CONN_ID", "financial_postgres")
LOOKBACK_DAYS = int(os.getenv("COLLECTOR_LOOKBACK_DAYS", "7"))
LOCAL_DATA_DIR = os.getenv("COLLECTOR_LOCAL_DATA_DIR", "/opt/airflow/data/landing")
# HNX currently has no verified BCTC listing URL for this collector. Keep it out
# of scheduled runs until a public report listing/endpoint has been identified.
ENABLED_SOURCES = ("ssc",)


@dag(
    dag_id="collect_financial_disclosures",
    description="Discover BCTC announcements from HNX and UBCKNN/SSC and register metadata in PostgreSQL",
    schedule="0 7 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Ho_Chi_Minh"),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "data-platform", "retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["bctc", "hnx", "ssc", "collector"],
)
def collect_financial_disclosures():
    @task
    def ensure_schema() -> None:
        sql_path = os.path.join(os.path.dirname(__file__), "..", "sql", "init.sql")
        with open(sql_path, encoding="utf-8") as sql_file:
            schema_sql = sql_file.read()
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        hook.run(schema_sql, autocommit=True)

    @task
    def collect_source(source: str) -> dict[str, int | str]:
        context = get_current_context()
        run_id = context["run_id"]
        dag_run = context.get("dag_run")
        run_conf = (dag_run.conf or {}) if dag_run else {}
        requested_sources = run_conf.get("sources")
        if requested_sources is not None:
            if not isinstance(requested_sources, list) or not set(requested_sources) <= set(ENABLED_SOURCES):
                raise ValueError(f"sources must be a list containing only enabled sources: {ENABLED_SOURCES}")
            if source not in requested_sources:
                raise AirflowSkipException(f"Source {source} excluded by this DAG run configuration")
        lookback_days = int(run_conf.get("lookback_days", LOOKBACK_DAYS))
        if not 1 <= lookback_days <= 3650:
            raise ValueError("lookback_days must be between 1 and 3650")
        max_downloads = run_conf.get("max_downloads")
        if max_downloads is not None:
            max_downloads = int(max_downloads)
            if not 1 <= max_downloads <= 1000:
                raise ValueError("max_downloads must be between 1 and 1000")

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    """INSERT INTO collector_runs (run_id, source, status)
                       VALUES (%s, %s, 'running')
                       ON CONFLICT (run_id) DO UPDATE SET source = EXCLUDED.source,
                           status = 'running', started_at = now(), finished_at = NULL,
                           error_message = NULL""",
                    (f"{run_id}:{source}", source),
                )
            conn.commit()

            records = collect_disclosures(
                source, lookback_days, LOCAL_DATA_DIR,
                max_downloads=max_downloads if source == "ssc" else None,
            )
            with conn.cursor() as cursor:
                for record in records:
                    cursor.execute(
                        """INSERT INTO financial_disclosures (
                               disclosure_id, source, source_url, ticker, title, report_type,
                               published_at, attachment_urls, local_files, raw_metadata, last_run_id,
                               last_seen_at, updated_at
                           ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, %s::jsonb, %s, now(), now())
                           ON CONFLICT (disclosure_id) DO UPDATE SET
                               ticker = COALESCE(EXCLUDED.ticker, financial_disclosures.ticker),
                               title = EXCLUDED.title,
                               report_type = COALESCE(EXCLUDED.report_type, financial_disclosures.report_type),
                               published_at = COALESCE(EXCLUDED.published_at, financial_disclosures.published_at),
                               attachment_urls = EXCLUDED.attachment_urls,
                               local_files = EXCLUDED.local_files,
                               raw_metadata = EXCLUDED.raw_metadata,
                               last_run_id = EXCLUDED.last_run_id,
                               last_seen_at = now(), updated_at = now()""",
                        (
                            record["disclosure_id"], record["source"], record["source_url"],
                            record["ticker"], record["title"], record["report_type"],
                            record["published_at"], json.dumps(record["attachment_urls"], ensure_ascii=False),
                            json.dumps(record["local_files"], ensure_ascii=False),
                            json.dumps(record["raw_metadata"], ensure_ascii=False), f"{run_id}:{source}",
                        ),
                    )
                cursor.execute(
                    """UPDATE collector_runs SET status = 'success', finished_at = now(),
                              records_seen = %s, records_upserted = %s, error_message = NULL
                       WHERE run_id = %s""",
                    (len(records), len(records), f"{run_id}:{source}"),
                )
            conn.commit()
            return {"source": source, "records_seen": len(records), "records_upserted": len(records)}
        except Exception as exc:
            conn.rollback()
            with conn.cursor() as cursor:
                cursor.execute(
                    """INSERT INTO collector_runs (run_id, source, status, finished_at, error_message)
                       VALUES (%s, %s, 'failed', now(), %s)
                       ON CONFLICT (run_id) DO UPDATE SET status = 'failed', finished_at = now(),
                           error_message = EXCLUDED.error_message""",
                    (f"{run_id}:{source}", source, str(exc)[:4000]),
                )
            conn.commit()
            raise
        finally:
            conn.close()

    schema_ready = ensure_schema()
    for source in ENABLED_SOURCES:
        schema_ready >> collect_source.override(task_id=f"collect_{source}")(source)


collect_financial_disclosures()
