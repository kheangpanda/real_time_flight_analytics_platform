"""
spark/streaming_job.py
━━━━━━━━━━━━━━━━━━━━━━
PySpark Structured Streaming ETL
  Kafka ──► raw layer ──► staging layer ──► main aggregation

Pipeline stages
───────────────
1. Read JSON messages from Kafka topic `flight_stream`
2. Parse + validate each record against the flight schema
3. Derive computed columns (velocity_kmh, altitude_level, flight_region)
4. Write raw records to PostgreSQL `raw.flight_events_raw`
5. Write clean records to PostgreSQL `staging.flight_events_clean`
6. Upsert per-aircraft aggregates into `main.flight_analytics`
7. Refresh `main.country_traffic_summary` and `main.hourly_flight_counts`
"""
from __future__ import annotations

import logging
import os
import time

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

# ─────────────────────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────────────────────
KAFKA_BROKER     = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC      = "flight_stream"

POSTGRES_HOST    = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT    = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB      = os.getenv("POSTGRES_DB", "flightdw")
POSTGRES_USER    = os.getenv("POSTGRES_USER", "flightuser")
POSTGRES_PASS    = os.getenv("POSTGRES_PASSWORD", "flightpass123")
JDBC_URL         = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
JDBC_PROPS       = {
    "user":     POSTGRES_USER,
    "password": POSTGRES_PASS,
    "driver":   "org.postgresql.Driver",
}

SPARK_MASTER     = os.getenv("SPARK_MASTER", "local[*]")
CHECKPOINT_BASE  = "/tmp/spark-checkpoints"
MICRO_BATCH_SEC  = 15   # seconds between micro-batches

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("spark-etl")

# ─────────────────────────────────────────────────────────────────────────────
# JSON schema (matches what the producer publishes)
# ─────────────────────────────────────────────────────────────────────────────
FLIGHT_SCHEMA = StructType([
    StructField("event_id",        StringType(),  True),
    StructField("icao24",          StringType(),  True),
    StructField("callsign",        StringType(),  True),
    StructField("origin_country",  StringType(),  True),
    StructField("time_position",   LongType(),    True),
    StructField("last_contact",    LongType(),    True),
    StructField("longitude",       DoubleType(),  True),
    StructField("latitude",        DoubleType(),  True),
    StructField("baro_altitude",   DoubleType(),  True),
    StructField("geo_altitude",    DoubleType(),  True),
    StructField("on_ground",       BooleanType(), True),
    StructField("velocity",        DoubleType(),  True),
    StructField("velocity_kmh",    DoubleType(),  True),
    StructField("true_track",      DoubleType(),  True),
    StructField("vertical_rate",   DoubleType(),  True),
    StructField("squawk",          StringType(),  True),
    StructField("spi",             BooleanType(), True),
    StructField("position_source", LongType(),    True),
    StructField("altitude_level",  StringType(),  True),
    StructField("flight_region",   StringType(),  True),
    StructField("event_timestamp", LongType(),    True),
])


# ─────────────────────────────────────────────────────────────────────────────
# SparkSession factory
# ─────────────────────────────────────────────────────────────────────────────
def build_spark() -> SparkSession:
    spark = (
        SparkSession.builder
        .master(SPARK_MASTER)
        .appName("FlightAnalyticsStreamingETL")
        # Kafka connector
        .config(
            "spark.jars.packages",
            ",".join([
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
                "org.postgresql:postgresql:42.7.1",
            ]),
        )
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_BASE)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.driver.memory", "1g")
        .config("spark.executor.memory", "1g")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ─────────────────────────────────────────────────────────────────────────────
# Read from Kafka
# ─────────────────────────────────────────────────────────────────────────────
def read_kafka_stream(spark: SparkSession) -> DataFrame:
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", 5000)
        .load()
    )


# ─────────────────────────────────────────────────────────────────────────────
# Parse + enrich
# ─────────────────────────────────────────────────────────────────────────────
def parse_and_enrich(raw_df: DataFrame) -> DataFrame:
    parsed = (
        raw_df
        .select(
            F.col("offset").alias("kafka_offset"),
            F.col("partition").alias("kafka_partition"),
            F.col("topic").alias("kafka_topic"),
            F.from_json(F.col("value").cast("string"), FLIGHT_SCHEMA).alias("d"),
        )
        .select("kafka_offset", "kafka_partition", "kafka_topic", "d.*")
    )

    # Re-compute velocity_kmh from velocity in case producer value differs
    enriched = parsed.withColumn(
        "velocity_kmh",
        F.when(F.col("velocity").isNotNull(), F.round(F.col("velocity") * 3.6, 2)),
    )

    # Classify altitude
    enriched = enriched.withColumn(
        "altitude_level",
        F.when(F.col("baro_altitude").isNull(), "unknown")
         .when(F.col("baro_altitude") <= 0, "ground")
         .when(F.col("baro_altitude") < 1000, "low")
         .when(F.col("baro_altitude") < 6000, "medium")
         .when(F.col("baro_altitude") < 12000, "high")
         .otherwise("very_high"),
    )

    # Classify flight region
    enriched = enriched.withColumn(
        "flight_region",
        F.when(
            F.col("latitude").isNull() | F.col("longitude").isNull(), "unknown"
        ).when(F.col("latitude") > 60, "arctic")
         .when(F.col("latitude") < -60, "antarctic")
         .when(
             (F.col("longitude").between(-30, 60)) & (F.col("latitude").between(0, 60)),
             "europe_africa",
         ).when(
             (F.col("longitude").between(60, 150)) & (F.col("latitude").between(0, 60)),
             "asia_pacific",
         ).when(
             (F.col("longitude").between(-180, -30)) & (F.col("latitude").between(0, 60)),
             "north_america",
         ).when(
             (F.col("longitude").between(-90, -30)) & (F.col("latitude").between(-60, 0)),
             "south_america",
         ).otherwise("other"),
    )

    # Drop records without icao24
    return enriched.filter(F.col("icao24").isNotNull())


# ─────────────────────────────────────────────────────────────────────────────
# Write helpers
# ─────────────────────────────────────────────────────────────────────────────
def _jdbc_write(df: DataFrame, table: str, mode: str = "append") -> None:
    """Write a static DataFrame to PostgreSQL via JDBC."""
    (
        df.write
        .format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", table)
        .option("user", POSTGRES_USER)
        .option("password", POSTGRES_PASS)
        .option("driver", "org.postgresql.Driver")
        .mode(mode)
        .save()
    )


# ─────────────────────────────────────────────────────────────────────────────
# Micro-batch processor
# ─────────────────────────────────────────────────────────────────────────────
def process_batch(batch_df: DataFrame, batch_id: int) -> None:
    if batch_df.isEmpty():
        logger.info("Batch %d — empty, skipping.", batch_id)
        return

    count = batch_df.count()
    logger.info("Batch %d — %d records", batch_id, count)

    # ── 1. RAW LAYER ─────────────────────────────────────────────────────────
    raw_df = batch_df.select(
        F.col("kafka_offset"),
        F.col("kafka_partition"),
        F.col("kafka_topic"),
        F.to_json(
            F.struct(*[c for c in batch_df.columns if c not in
                       ("kafka_offset", "kafka_partition", "kafka_topic")])
        ).alias("payload"),
        F.current_timestamp().alias("received_at"),
    )
    _jdbc_write(raw_df, "raw.flight_events_raw")

    # ── 2. STAGING LAYER ─────────────────────────────────────────────────────
    staging_df = batch_df.select(
        # event_id intentionally omitted: PostgreSQL UUID column rejects VARCHAR
        # from JDBC StringType. The DEFAULT uuid_generate_v4() fills it instead.
        # The producer's event_id is preserved in raw.flight_events_raw.payload.
        "icao24", "callsign", "origin_country",
        "longitude", "latitude",
        "baro_altitude", "geo_altitude",
        "velocity", "velocity_kmh",
        "true_track", "vertical_rate",
        "on_ground", "squawk", "spi", "position_source",
        "altitude_level", "flight_region",
        "time_position", "last_contact",
        F.current_timestamp().alias("ingested_at"),
        F.lit(True).alias("is_valid"),
    )
    _jdbc_write(staging_df, "staging.flight_events_clean")

    # ── 3. MAIN AGGREGATION ───────────────────────────────────────────────────
    # Per-aircraft latest + aggregates in this micro-batch
    window_start = F.lit(
        batch_df.select(
            F.from_unixtime(F.min("event_timestamp")).cast("timestamp")
        ).collect()[0][0]
    )
    window_end = F.lit(
        batch_df.select(
            F.from_unixtime(F.max("event_timestamp")).cast("timestamp")
        ).collect()[0][0]
    )

    main_df = (
        batch_df
        .groupBy("icao24", "callsign", "origin_country", "altitude_level", "flight_region")
        .agg(
            F.last("longitude",             ignorenulls=True).alias("longitude"),
            F.last("latitude",              ignorenulls=True).alias("latitude"),
            F.last("baro_altitude",         ignorenulls=True).alias("baro_altitude"),
            F.avg("velocity_kmh")                            .alias("avg_velocity_kmh"),
            F.max("velocity_kmh")                            .alias("max_velocity_kmh"),
            F.avg("baro_altitude")                          .alias("avg_altitude"),
            F.max("baro_altitude")                          .alias("max_altitude"),
            F.avg("vertical_rate")                          .alias("avg_vertical_rate"),
            # F.count / F.sum return LongType; cast to INT to match PostgreSQL INT column
            F.count("*").cast("int")                        .alias("observation_count"),
            F.sum(F.when(F.col("on_ground") == True, 1).otherwise(0)).cast("int").alias("on_ground_count"),
        )
        .withColumn("window_start",   window_start)
        .withColumn("window_end",     window_end)
        .withColumn("last_updated",   F.current_timestamp())
        .withColumn("predicted_velocity", F.lit(None).cast(DoubleType()))
        .withColumn("anomaly_score",      F.lit(None).cast(DoubleType()))
        .withColumn("cluster_id",         F.lit(None).cast(LongType()).cast("int"))
    )
    _jdbc_write(main_df, "main.flight_analytics")

    # ── 4. COUNTRY SUMMARY ────────────────────────────────────────────────────
    country_df = (
        batch_df
        .groupBy("origin_country")
        .agg(
            F.countDistinct("icao24").cast("int").alias("flight_count"),
            F.avg("baro_altitude").alias("avg_altitude"),
            F.avg("velocity_kmh").alias("avg_velocity"),
        )
        .withColumn("snapshot_time", F.current_timestamp())
    )
    _jdbc_write(country_df, "main.country_traffic_summary")

    # ── 5. HOURLY COUNTS ─────────────────────────────────────────────────────
    hourly_df = (
        batch_df
        .withColumn("bucket",
            F.date_trunc("hour",
                F.to_timestamp(F.from_unixtime(F.col("event_timestamp")))
            )
        )
        .groupBy("bucket")
        .agg(
            F.countDistinct("icao24").cast("int").alias("flight_count"),
            F.avg("velocity_kmh").alias("avg_velocity"),
            F.avg("baro_altitude").alias("avg_altitude"),
        )
    )
    _jdbc_write(hourly_df, "main.hourly_flight_counts")

    logger.info("Batch %d written to all layers.", batch_id)


# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────
def main() -> None:
    logger.info("🚀 Spark Structured Streaming ETL starting…")
    logger.info("   Kafka  : %s / %s", KAFKA_BROKER, KAFKA_TOPIC)
    logger.info("   PG     : %s", JDBC_URL)

    spark  = build_spark()
    raw_df = read_kafka_stream(spark)
    etl_df = parse_and_enrich(raw_df)

    query = (
        etl_df.writeStream
        .outputMode("update")
        .trigger(processingTime=f"{MICRO_BATCH_SEC} seconds")
        .foreachBatch(process_batch)
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/flight_etl")
        .start()
    )

    logger.info("Streaming query started. Awaiting termination…")
    query.awaitTermination()


if __name__ == "__main__":
    # Give Kafka time to be ready before connecting
    time.sleep(30)
    main()
