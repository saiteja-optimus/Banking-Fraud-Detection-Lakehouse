from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# --- Silver: Enriched Transactions (with late-arriving data detection) ---
@dp.table(
    name="transactions_enriched",
    comment="Transactions enriched with merchant data and fraud indicators"
)
@dp.expect_or_drop("valid_customer", "customer_id IS NOT NULL")
@dp.expect_or_drop("valid_merchant", "merchant_id IS NOT NULL")
@dp.expect("valid_amount_range", "amount BETWEEN 0.01 AND 100000")
@dp.expect("timely_data", "CAST(timestamp AS TIMESTAMP) >= CURRENT_TIMESTAMP() - INTERVAL 24 HOURS")
def transactions_enriched():
    transactions = spark.readStream.table("transactions_bronze")
    merchants = spark.read.table("merchants_bronze")

    return (
        transactions
            .join(merchants, "merchant_id", "left")
            .withColumn("transaction_hour", hour(col("timestamp").cast("timestamp")))
            .withColumn("transaction_day_of_week", dayofweek(col("timestamp").cast("timestamp")))
            .withColumn("is_high_value", col("amount") > 5000)
            .withColumn("is_international", col("location_country") != lit("US"))
            .select(
                "transaction_id",
                "customer_id",
                "merchant_id",
                "merchant_name",
                "category",
                "amount",
                "currency",
                "transaction_type",
                "channel",
                "timestamp",
                "location_country",
                "location_city",
                "card_last_four",
                "is_international",
                "transaction_hour",
                "transaction_day_of_week",
                "is_high_value",
                "risk_score"
            )
    )

# --- Silver: Deduplicated Transactions ---
@dp.materialized_view(
    name="transactions_deduped",
    comment="Deduplicated transactions - latest record per transaction_id"
)
@dp.expect("no_duplicates", "row_num = 1")
def transactions_deduped():
    window_spec = Window.partitionBy("transaction_id").orderBy(col("timestamp").desc())

    return (
        spark.read.table("transactions_enriched")
            .withColumn("row_num", row_number().over(window_spec))
            .filter(col("row_num") == 1)
            # Remove or comment out this line:
            # .drop("row_num") 
    )