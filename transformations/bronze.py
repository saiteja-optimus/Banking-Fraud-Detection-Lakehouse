from pyspark import pipelines as dp
from pyspark.sql.functions import *

# Volume path for raw data
VOLUME_PATH = "/Volumes/fraud_detection/lakehouse/raw_data"

# --- Bronze: Transactions (with schema evolution) ---
@dp.table(
    name="transactions_bronze",
    comment="Raw card transactions ingested via Auto Loader from landing zone"
)
@dp.expect_or_drop("valid_transaction_id", "transaction_id IS NOT NULL")
@dp.expect("valid_amount", "amount > 0")
@dp.expect("valid_timestamp", "timestamp IS NOT NULL")
def transactions_bronze():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.inferColumnTypes", "true")
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            .option("mergeSchema", "true")
            .load(f"{VOLUME_PATH}/transactions/")
    )

    # --- Bronze: Customers CDC ---
dp.create_streaming_table(
    name="customers_cdc_bronze",
    comment="Raw customer CDC events ingested via Auto Loader"
)

@dp.append_flow(
    target="customers_cdc_bronze",
    name="customers_cdc_ingest_flow"
)
def customers_cdc_ingest_flow():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.inferColumnTypes", "true")
            .load(f"{VOLUME_PATH}/customers/")
    )

    # --- Bronze: Merchants (Reference Data) ---
@dp.materialized_view(
    name="merchants_bronze",
    comment="Merchant reference data - batch loaded"
)
@dp.expect_or_drop("valid_merchant_id", "merchant_id IS NOT NULL")
@dp.expect("valid_risk_score", "risk_score BETWEEN 0 AND 1")
def merchants_bronze():
    return (
        spark.read
            .format("json")
            .option("inferSchema", "true")
            .load(f"{VOLUME_PATH}/merchants/")
    )

    