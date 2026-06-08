import os
import shutil
from pathlib import Path

import kagglehub
from prefect import flow, task
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

BASE_DIR             = Path(__file__).parent
DATA_DIR             = BASE_DIR / "data"
DELTA_BRONZE         = str(DATA_DIR / "delta" / "bronze_credit_card")
DELTA_SILVER         = str(DATA_DIR / "delta" / "silver_credit_card")
DELTA_SILVER_PART    = str(DATA_DIR / "delta" / "silver_credit_card_partitioned")
DELTA_GOLD_FLAGGED   = str(DATA_DIR / "delta" / "gold_flagged_transactions")
DELTA_GOLD_SUMMARY   = str(DATA_DIR / "delta" / "gold_batch_summary")
CHECKPOINT           = str(DATA_DIR / "checkpoints" / "gold")

EPOCH_OFFSET = 1_735_689_600  # Jan 1 2025 00:00:00 UTC


def get_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("credit_card_fraud_pipeline")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.jars.packages", "io.delta:delta-spark_2.13:4.1.0")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )


@task(name="ingest-bronze", log_prints=True)
def ingest_bronze() -> int:
    spark = get_spark()
    (DATA_DIR / "delta").mkdir(parents=True, exist_ok=True)
    (DATA_DIR / "checkpoints").mkdir(parents=True, exist_ok=True)

    dataset_path = kagglehub.dataset_download("mlg-ulb/creditcardfraud")
    raw_csv = Path(dataset_path) / "creditcard.csv"
    print(f"Dataset path: {raw_csv}")

    df_raw = spark.read.csv(str(raw_csv), header=True, inferSchema=True)
    row_count = df_raw.count()
    print(f"Rows: {row_count:,}  Columns: {len(df_raw.columns)}")

    df_raw.write.format("delta").mode("overwrite").save(DELTA_BRONZE)
    print(f"Written to {DELTA_BRONZE}")
    df_raw.show(5)
    return row_count


@task(name="transform-silver", log_prints=True)
def transform_silver() -> int:
    spark = get_spark()
    df_bronze = spark.read.format("delta").load(DELTA_BRONZE)
    print(f"Bronze rows: {df_bronze.count():,}")

    df_silver = df_bronze.withColumn(
        "event_time",
        (F.col("Time").cast("long") + EPOCH_OFFSET).cast("timestamp"),
    )

    df_silver.write.format("delta").mode("overwrite").save(DELTA_SILVER)
    print(f"Written to {DELTA_SILVER}")
    df_silver.select("Time", "event_time", "Amount", "Class").show(10)
    return df_silver.count()


@task(name="partition-silver", log_prints=True)
def partition_silver() -> None:
    spark = get_spark()
    df_silver = spark.read.format("delta").load(DELTA_SILVER)
    df_silver.repartition(10).write.format("delta").mode("overwrite").save(DELTA_SILVER_PART)
    print("Silver repartitioned into 10 files\n")


def process_batch(batch_df, batch_id: int) -> None:
    total             = batch_df.count()
    total_amount      = batch_df.agg(F.sum("Amount")).collect()[0][0] or 0
    suspicious_df        = batch_df.filter(F.col("Class") == 1)
    fraud_count       = suspicious_df.count()
    fraud_rate        = round(fraud_count / total * 100, 2) if total > 0 else 0
    suspicious_amount = suspicious_df.agg(F.sum("Amount")).collect()[0][0] or 0

    print(f"Batch {batch_id}: {total} transactions, {fraud_count} suspicious "
          f"({fraud_rate}%), suspicious amount: ${round(suspicious_amount, 2)}")

    if fraud_count > 0:
        suspicious_df.withColumn("batch_id", F.lit(batch_id)) \
                      .write.format("delta").mode("append").save(DELTA_GOLD_FLAGGED)

    summary_df = batch_df.sparkSession.createDataFrame(
        [(batch_id, total, float(total_amount), fraud_count, fraud_rate, float(suspicious_amount))],
        schema="batch_id int, total_transactions long, total_amount double, fraud_count long, fraud_rate_pct double, suspicious_amount double"
    )
    summary_df.write.format("delta").mode("append").save(DELTA_GOLD_SUMMARY)


@task(name="stream-gold", log_prints=True)
def stream_gold() -> None:
    spark = get_spark()
    if os.path.exists(CHECKPOINT):
        shutil.rmtree(CHECKPOINT)

    stream_df = (
        spark.readStream
        .format("delta")
        .option("maxFilesPerTrigger", 1)
        .load(DELTA_SILVER_PART)
        .withWatermark("event_time", "10 minutes")
    )

    query = (
        stream_df.writeStream
        .foreachBatch(process_batch)
        .option("checkpointLocation", CHECKPOINT)
        .trigger(availableNow=True)
        .start()
    )

    query.awaitTermination()
    print("\nStream complete")


@task(name="print-results", log_prints=True)
def print_results() -> None:
    spark = get_spark()
    print("\n── Batch summaries ──────────────────────────────────────────────")
    spark.read.format("delta").load(DELTA_GOLD_SUMMARY).orderBy("batch_id").show()

    print("\n── Flagged transactions (sample) ────────────────────────────────")
    spark.read.format("delta").load(DELTA_GOLD_FLAGGED).show(5)


@flow(name="credit-card-fraud-pipeline", log_prints=True)
def credit_card_fraud_pipeline() -> None:
    get_spark().sparkContext.setLogLevel("WARN")

    try:
        ingest_bronze()
        transform_silver()
        partition_silver()
        stream_gold()
        print_results()
    finally:
        get_spark().stop()


if __name__ == "__main__":
    credit_card_fraud_pipeline()
