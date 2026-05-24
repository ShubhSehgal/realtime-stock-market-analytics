import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    abs as spark_abs,
    avg,
    col,
    count,
    from_json,
    max as spark_max,
    min as spark_min,
    sum as spark_sum,
    to_timestamp,
    window,
)
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "stock_ticks")
POSTGRES_URL = os.getenv("POSTGRES_URL", "jdbc:postgresql://localhost:5432/market")
POSTGRES_USER = os.getenv("POSTGRES_USER", "market_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "changeme")

schema = StructType(
    [
        StructField("symbol", StringType(), False),
        StructField("price", DoubleType(), False),
        StructField("volume", IntegerType(), False),
        StructField("event_time", StringType(), False),
    ]
)


def write_to_postgres(batch_df, batch_id: int, table_name: str) -> None:
    if batch_df.rdd.isEmpty():
        return

    (
        batch_df.write.format("jdbc")
        .option("url", POSTGRES_URL)
        .option("dbtable", table_name)
        .option("user", POSTGRES_USER)
        .option("password", POSTGRES_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .mode("append")
        .save()
    )


def main() -> None:
    spark = (
        SparkSession.builder.appName("RealTimeFinancialAnalytics")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )

    ticks = (
        raw_stream.selectExpr("CAST(value AS STRING) AS json_value")
        .select(from_json(col("json_value"), schema).alias("data"))
        .select("data.*")
        .withColumn("event_time", to_timestamp(col("event_time")))
        .withWatermark("event_time", "2 minutes")
    )

    (
        ticks.select("symbol", "price", "volume", "event_time")
        .writeStream.foreachBatch(lambda df, batch_id: write_to_postgres(df, batch_id, "stock_ticks"))
        .outputMode("append")
        .option("checkpointLocation", "/tmp/checkpoints/stock_ticks")
        .start()
    )

    metrics = (
        ticks.groupBy(window(col("event_time"), "1 minute"), col("symbol"))
        .agg(
            avg("price").alias("avg_price"),
            spark_min("price").alias("min_price"),
            spark_max("price").alias("max_price"),
            spark_sum("volume").alias("total_volume"),
            count("*").alias("tick_count"),
        )
        .select(
            col("symbol"),
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("avg_price"),
            col("min_price"),
            col("max_price"),
            col("total_volume"),
            col("tick_count"),
        )
    )

    (
        metrics.writeStream.foreachBatch(lambda df, batch_id: write_to_postgres(df, batch_id, "stock_metrics"))
        .outputMode("update")
        .option("checkpointLocation", "/tmp/checkpoints/stock_metrics")
        .start()
    )

    anomalies = (
        metrics
        .withColumn(
            "pct_deviation",
            spark_abs((col("max_price") - col("avg_price")) / col("avg_price"))
        )
        .filter(col("pct_deviation") >= 0.03)
        .select(
            col("symbol"),
            col("max_price").alias("price"),
            col("avg_price"),
            col("pct_deviation"),
            col("window_end").alias("event_time"),
        )
    )

    (
        anomalies.writeStream.foreachBatch(lambda df, batch_id: write_to_postgres(df, batch_id, "stock_anomalies"))
        .outputMode("update")
        .option("checkpointLocation", "/tmp/checkpoints/stock_anomalies")
        .start()
    )

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
