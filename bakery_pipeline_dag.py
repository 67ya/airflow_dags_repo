"""
Daily Bakery Order Pipeline DAG

Pipeline: @ORDERS_STAGE -> ORDERS_STG -> CUSTOMER_ORDERS -> SUMMARY_ORDERS

Schedule: daily at 06:00
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

SNOWFLAKE_CONN_ID = "snowflake_default"
BAKERY_DB         = "BAKERY_DB"
BAKERY_SCHEMA     = "ORDERS"
BAKERY_WH         = "BAKERY_WH"

DEFAULT_ARGS = {
    "owner":               "bakery-pipeline",
    "depends_on_past":     False,
    "start_date":          datetime(2024, 7, 1),
    "retries":             1,
    "retry_delay":         timedelta(minutes=5),
    "email_on_failure":    False,
    "email_on_retry":      False,
}

with DAG(
    dag_id="bakery_daily_pipeline",
    default_args=DEFAULT_ARGS,
    description="Daily Bakery Order ETL: Stage → Staging → Warehouse → Delivery",
    schedule="0 6 * * *",          # every day at 06:00
    catchup=False,
    tags=["bakery", "etl", "snowflake"],
) as dag:

    # ── 0. Health check ──────────────────────────────────────────────
    t0_health = SnowflakeOperator(
        task_id="health_check",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=f"""
            USE ROLE SYSADMIN;
            USE DATABASE {BAKERY_DB};
            USE SCHEMA {BAKERY_SCHEMA};
            USE WAREHOUSE {BAKERY_WH};
            SELECT CURRENT_TIMESTAMP() AS pipeline_start,
                   CURRENT_USER()      AS run_by;
        """,
    )

    # ── 1. Ingest: @ORDERS_STAGE → ORDERS_STG ───────────────────────
    t1_ingest = SnowflakeOperator(
        task_id="ingest_stage_to_stg",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=f"""
            USE DATABASE {BAKERY_DB};
            USE SCHEMA {BAKERY_SCHEMA};
            USE WAREHOUSE {BAKERY_WH};

            -- clear staging table before each load
            TRUNCATE TABLE ORDERS_STG;

            -- load CSV files from internal stage
            CALL load_file();
        """,
    )

    # ── 2. Check staging has data ────────────────────────────────────
    t2_check_stg = SnowflakeOperator(
        task_id="check_staging_rows",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=f"""
            USE DATABASE {BAKERY_DB};
            USE SCHEMA {BAKERY_SCHEMA};
            SELECT COUNT(*) AS stg_row_count FROM ORDERS_STG;
        """,
    )

    # ── 3. Transform: ORDERS_STG → CUSTOMER_ORDERS (SCD merge) ──────
    t3_transform_wh = SnowflakeOperator(
        task_id="transform_stg_to_warehouse",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=f"""
            USE DATABASE {BAKERY_DB};
            USE SCHEMA {BAKERY_SCHEMA};
            USE WAREHOUSE {BAKERY_WH};
            CALL transform_warehouse();
        """,
    )

    # ── 4. Transform: CUSTOMER_ORDERS → SUMMARY_ORDERS ──────────────
    t4_transform_summary = SnowflakeOperator(
        task_id="transform_warehouse_to_summary",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=f"""
            USE DATABASE {BAKERY_DB};
            USE SCHEMA {BAKERY_SCHEMA};
            USE WAREHOUSE {BAKERY_WH};

            -- remove today's existing summary before insert
            DELETE FROM SUMMARY_ORDERS
            WHERE delivery_date = CURRENT_DATE();

            CALL Transform_Daily_Summary();
        """,
    )

    # ── 5. Validate delivery table ───────────────────────────────────
    t5_validate = SnowflakeOperator(
        task_id="validate_delivery",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=f"""
            USE DATABASE {BAKERY_DB};
            USE SCHEMA {BAKERY_SCHEMA};

            -- final delivery snapshot
            SELECT
                delivery_date,
                baked_good_type,
                total_quantity
            FROM SUMMARY_ORDERS
            ORDER BY delivery_date DESC, total_quantity DESC
            LIMIT 20;
        """,
    )

    # ── DAG dependency chain ─────────────────────────────────────────
    # health_check → ingest → check_rows → transform_wh → transform_summary → validate
    t0_health >> t1_ingest >> t2_check_stg >> t3_transform_wh >> t4_transform_summary >> t5_validate
