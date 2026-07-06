from pyspark import pipelines as dp
from pyspark.sql.functions import *

# --- Silver CDC: Clean customer CDC events ---
dp.create_streaming_table(
    name="customers_cdc_clean",
    comment="Cleaned customer CDC events with quality enforcement",
    expect_all_or_drop={
        "valid_id": "id IS NOT NULL",
        "valid_operation": "operation IN ('APPEND', 'DELETE', 'UPDATE')",
        "no_rescued_data": "_rescued_data IS NULL"
    }
)

@dp.append_flow(
    target="customers_cdc_clean",
    name="customers_cdc_clean_flow"
)
def customers_cdc_clean_flow():
    return (
        spark.readStream.table("customers_cdc_bronze")
            .select(
                "id",
                "firstname",
                "lastname",
                "email",
                "phone",
                "address",
                "risk_category",
                "account_status",
                "operation",
                "operation_date",
                "_rescued_data"
            )
    )

# --- Silver CDC: Current Customer State (SCD Type 1) ---
dp.create_streaming_table(
    name="customers_current",
    comment="Current state of each customer - SCD Type 1"
)

dp.create_auto_cdc_flow(
    target="customers_current",
    source="customers_cdc_clean",
    keys=["id"],
    sequence_by=col("operation_date"),
    ignore_null_updates=False,
    apply_as_deletes=expr("operation = 'DELETE'"),
    except_column_list=["operation", "operation_date", "_rescued_data"],
)

# --- Silver CDC: Customer History (SCD Type 2) ---
dp.create_streaming_table(
    name="customers_history",
    comment="Full customer change history - SCD Type 2"
)

dp.create_auto_cdc_flow(
    target="customers_history",
    source="customers_cdc_clean",
    keys=["id"],
    sequence_by=col("operation_date"),
    ignore_null_updates=False,
    apply_as_deletes=expr("operation = 'DELETE'"),
    except_column_list=["operation", "operation_date", "_rescued_data"],
    stored_as_scd_type="2",
)