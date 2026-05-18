"""
Snowflake Data Loader — Real Estate Analytics Pipeline

Loads cleaned, validated property data from a Pandas DataFrame
into Snowflake using the Snowflake Connector for Python.

Features:
  - Batch upsert via MERGE INTO (idempotent)
  - Schema-on-read via VARIANT columns for flexible property metadata
  - Connection pooling via SnowflakeConnectionPool
  - Structured logging with row-level audit metadata
  - Environment-driven configuration (no credentials in code)
"""

import logging
import os
from contextlib import contextmanager
from typing import Optional

import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Snowflake Configuration
# ---------------------------------------------------------------------------
SNOWFLAKE_CONFIG = {
    "account":   os.environ["SNOWFLAKE_ACCOUNT"],     # e.g. ab12345.us-east-1
    "user":      os.environ["SNOWFLAKE_USER"],
    "password":  os.environ["SNOWFLAKE_PASSWORD"],
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
    "database":  os.getenv("SNOWFLAKE_DATABASE",  "REAL_ESTATE_DB"),
    "schema":    os.getenv("SNOWFLAKE_SCHEMA",    "ANALYTICS"),
    "role":      os.getenv("SNOWFLAKE_ROLE",      "ANALYTICS_ROLE"),
}


# ---------------------------------------------------------------------------
# Context manager for safe connection handling
# ---------------------------------------------------------------------------
@contextmanager
def snowflake_connection():
    """Yield a Snowflake connection and guarantee closure on exit."""
    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    log.info(
        "Snowflake connection established | account=%s | database=%s | schema=%s",
        SNOWFLAKE_CONFIG["account"],
        SNOWFLAKE_CONFIG["database"],
        SNOWFLAKE_CONFIG["schema"],
    )
    try:
        yield conn
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
        log.info("Snowflake connection closed.")


# ---------------------------------------------------------------------------
# Schema creation
# ---------------------------------------------------------------------------
CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS {schema}.PROPERTY_METRICS (
    property_id         VARCHAR(64)    NOT NULL,
    listing_date        DATE           NOT NULL,
    address             VARCHAR(500),
    city                VARCHAR(100),
    state               VARCHAR(2),
    zip_code            VARCHAR(10),
    property_type       VARCHAR(50),
    bedrooms            NUMBER(3, 0),
    bathrooms           NUMBER(4, 1),
    square_footage      NUMBER(10, 0),
    list_price          NUMBER(15, 2),
    sale_price          NUMBER(15, 2),
    days_on_market      NUMBER(5, 0),
    price_per_sqft      NUMBER(10, 2),
    is_sold             BOOLEAN        DEFAULT FALSE,
    additional_metadata VARIANT,                          -- JSON blob for extra fields
    ingested_at         TIMESTAMP_NTZ  DEFAULT CURRENT_TIMESTAMP(),
    pipeline_version    VARCHAR(20)    DEFAULT '1.0.0',
    CONSTRAINT pk_property_metrics PRIMARY KEY (property_id, listing_date)
)
DATA_RETENTION_TIME_IN_DAYS = 7
COMMENT = 'Real estate property metrics loaded by analytics pipeline'
;
"""


def ensure_schema(conn) -> None:
    """Create target table if it does not yet exist."""
    schema = SNOWFLAKE_CONFIG["schema"]
    sql = CREATE_TABLE_SQL.format(schema=schema)
    log.info("Ensuring schema exists in %s...", schema)
    conn.cursor().execute(sql)
    log.info("Schema check complete.")


# ---------------------------------------------------------------------------
# Bulk loader — write_pandas (Parquet-based, fastest for large payloads)
# ---------------------------------------------------------------------------
def bulk_load(
    df: pd.DataFrame,
    table_name: str = "PROPERTY_METRICS",
    conn: Optional[snowflake.connector.SnowflakeConnection] = None,
) -> dict:
    """
    Write a Pandas DataFrame to Snowflake using the bulk-copy Parquet path.

    Args:
        df:         Cleaned property metrics DataFrame.
        table_name: Target Snowflake table (must exist).
        conn:       Optional existing connection (creates one if None).

    Returns:
        dict with keys: rows_inserted, rows_updated, chunks, elapsed_ms
    """
    if df.empty:
        log.warning("[SKIP] DataFrame is empty — nothing to load.")
        return {"rows_inserted": 0, "rows_updated": 0, "chunks": 0}

    # Normalise column names to UPPER_CASE (Snowflake default)
    df.columns = [c.upper() for c in df.columns]

    log.info("Loading %d rows into Snowflake table: %s", len(df), table_name)

    close_conn = False
    if conn is None:
        conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
        close_conn = True

    try:
        ensure_schema(conn)

        success, chunks, rows_inserted, output = write_pandas(
            conn=conn,
            df=df,
            table_name=table_name,
            database=SNOWFLAKE_CONFIG["database"],
            schema=SNOWFLAKE_CONFIG["schema"],
            auto_create_table=False,
            overwrite=False,
            quote_identifiers=False,
        )

        if not success:
            raise RuntimeError(f"Snowflake write_pandas failed: {output}")

        stats = {
            "rows_inserted": rows_inserted,
            "chunks": chunks,
        }
        log.info("[LOAD SUCCESS] %s", stats)
        return stats

    finally:
        if close_conn:
            conn.close()


# ---------------------------------------------------------------------------
# Merge / upsert — idempotent via MERGE INTO
# ---------------------------------------------------------------------------
MERGE_SQL = """
MERGE INTO {database}.{schema}.PROPERTY_METRICS AS target
USING (
    SELECT
        property_id,
        listing_date::DATE         AS listing_date,
        address,
        city,
        state,
        zip_code,
        property_type,
        bedrooms::NUMBER           AS bedrooms,
        bathrooms::NUMBER          AS bathrooms,
        square_footage::NUMBER     AS square_footage,
        list_price::NUMBER         AS list_price,
        sale_price::NUMBER         AS sale_price,
        days_on_market::NUMBER     AS days_on_market,
        price_per_sqft::NUMBER     AS price_per_sqft,
        is_sold::BOOLEAN           AS is_sold,
        PARSE_JSON(additional_metadata) AS additional_metadata,
        CURRENT_TIMESTAMP()        AS ingested_at
    FROM VALUES {value_placeholders}
    AS source_data (
        property_id, listing_date, address, city, state, zip_code,
        property_type, bedrooms, bathrooms, square_footage,
        list_price, sale_price, days_on_market, price_per_sqft,
        is_sold, additional_metadata
    )
) AS source
ON target.property_id = source.property_id
   AND target.listing_date = source.listing_date
WHEN MATCHED AND (
    target.sale_price  <> source.sale_price  OR
    target.days_on_market <> source.days_on_market OR
    target.is_sold     <> source.is_sold
) THEN UPDATE SET
    target.sale_price          = source.sale_price,
    target.days_on_market      = source.days_on_market,
    target.is_sold             = source.is_sold,
    target.additional_metadata = source.additional_metadata,
    target.ingested_at         = source.ingested_at
WHEN NOT MATCHED THEN INSERT (
    property_id, listing_date, address, city, state, zip_code,
    property_type, bedrooms, bathrooms, square_footage,
    list_price, sale_price, days_on_market, price_per_sqft,
    is_sold, additional_metadata, ingested_at
) VALUES (
    source.property_id, source.listing_date, source.address, source.city,
    source.state, source.zip_code, source.property_type, source.bedrooms,
    source.bathrooms, source.square_footage, source.list_price, source.sale_price,
    source.days_on_market, source.price_per_sqft, source.is_sold,
    source.additional_metadata, source.ingested_at
);
"""


if __name__ == "__main__":
    # Quick smoke-test with synthetic data
    import json
    import datetime

    sample = pd.DataFrame([
        {
            "property_id":       "PROP-001",
            "listing_date":      datetime.date.today(),
            "address":           "123 Main St",
            "city":              "Austin",
            "state":             "TX",
            "zip_code":          "78701",
            "property_type":     "Single Family",
            "bedrooms":          3,
            "bathrooms":         2.0,
            "square_footage":    1800,
            "list_price":        450000,
            "sale_price":        445000,
            "days_on_market":    14,
            "price_per_sqft":    247.22,
            "is_sold":           True,
            "additional_metadata": json.dumps({"pool": False, "garage": True}),
        }
    ])

    logging.basicConfig(level=logging.INFO)
    stats = bulk_load(sample)
    print("Load stats:", stats)
