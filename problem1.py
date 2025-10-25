#!/usr/bin/env python3
"""
Problem 1: Log Level Distribution (Cluster Version)
Author: jp2132 (final version with summary.txt)
"""

import sys
import os
import time
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_extract, rand, count

# ---------------- Logging Configuration ----------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# ---------------- Spark Session ----------------
def create_spark_session(master_url):
    spark = (
        SparkSession.builder
        .appName("Problem1_LogLevel_Distribution_Cluster")
        .master(master_url)
        # Core resource configuration
        .config("spark.executor.memory", "4g")
        .config("spark.driver.memory", "4g")
        .config("spark.executor.cores", "2")
        .config("spark.cores.max", "6")
        # Load Hadoop AWS dependencies (for S3A support)
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262"
        )
        # Hadoop S3A implementation configuration (critical)
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")  # Compatible with old s3:// scheme
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true")
        .config("spark.hadoop.fs.s3a.connection.timeout", "5000")
        # Performance optimization
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    logger.info("Spark session created successfully (S3A enabled)")
    return spark

# ---------------- Analysis Logic ----------------
def analyze_logs(spark, data_path, output_prefix):
    start = time.time()
    logger.info(f"Reading logs from: {data_path}")

    # Automatically recurse through all folders
    df_raw = spark.read.option("recursiveFileLookup", "true").text(data_path)
    total_lines = df_raw.count()
    logger.info(f"Total log lines read: {total_lines:,}")

    log_pattern = r"\b(INFO|WARN|ERROR|DEBUG)\b"
    df_levels = df_raw.withColumn("log_level", regexp_extract(col("value"), log_pattern, 1))
    df_valid = df_levels.filter(col("log_level") != "")

    # Aggregate statistics
    df_counts = (
        df_valid.groupBy("log_level")
        .agg(count("*").alias("count"))
        .orderBy(col("count").desc())
    )

    # Random sample
    df_sample = (
        df_valid.select(col("value").alias("log_entry"), "log_level")
        .orderBy(rand())
        .limit(10)
    )

    # Save results (CSV)
    logger.info("Writing CSV results to S3...")
    df_counts.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{output_prefix}/problem1_counts")
    df_sample.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{output_prefix}/problem1_sample")

    # ----------- Generate Summary Text -----------
    logger.info("Generating summary report...")
    total_with_level = df_valid.count()
    unique_levels = [r["log_level"] for r in df_counts.collect()]
    total_sum = df_counts.agg({"count": "sum"}).collect()[0][0]

    summary_lines = [
        f"Total log lines processed: {total_lines:,}",
        f"Total lines with log levels: {total_with_level:,}",
        f"Unique log levels found: {len(unique_levels)}",
        "",
        "Log level distribution:"
    ]

    for row in df_counts.collect():
        pct = (row['count'] / total_sum) * 100
        summary_lines.append(f"  {row['log_level']:6}: {row['count']:10,d} ({pct:6.2f}%)")

    summary_text = "\n".join(summary_lines)
    logger.info("\n" + summary_text)

    # Write summary to a local temporary file
    local_summary_path = "/tmp/problem1_summary.txt"
    with open(local_summary_path, "w") as f:
        f.write(summary_text)

    # Upload to S3
    summary_s3_path = f"{output_prefix}/problem1_summary.txt"
    os.system(f"aws s3 cp {local_summary_path} {summary_s3_path}")

    logger.info(f"Summary file uploaded to: {summary_s3_path}")

    logger.info(f"Analysis completed in {(time.time() - start)/60:.2f} min")

# ---------------- Entry Point ----------------
def main():
    if len(sys.argv) != 2:
        print("Usage: python problem1_cluster.py spark://<MASTER_PRIVATE_IP>:7077")
        sys.exit(1)

    master_url = sys.argv[1]
    spark = create_spark_session(master_url)

    # Automatically read S3 bucket from environment variable
    bucket = os.environ.get("SPARK_LOGS_BUCKET")
    if not bucket:
        raise EnvironmentError("SPARK_LOGS_BUCKET environment variable not found!")

    # Force replacement with s3a:// protocol
    bucket = bucket.replace("s3://", "s3a://")

    data_path = f"{bucket}/data/"
    output_prefix = f"{bucket}/output"

    logger.info(f"Using data path: {data_path}")
    logger.info(f"Writing output to: {output_prefix}")

    analyze_logs(spark, data_path, output_prefix)

    spark.stop()
    logger.info("Spark session stopped.")

if __name__ == "__main__":
    main()
