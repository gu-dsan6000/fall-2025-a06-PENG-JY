#!/usr/bin/env python3
"""
Problem 2: Cluster Usage Analysis (Full Cluster Version)
Author: jp2132
"""

import os
import sys
import re
import argparse
import logging
from datetime import datetime
from io import StringIO
import pandas as pd
import matplotlib
matplotlib.use("Agg")  
import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, regexp_extract, min as spark_min, max as spark_max, countDistinct

# ---------------- Logging ----------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# ---------------- Helper ----------------
time_pattern = r"(\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})"

def parse_datetime(s):
    try:
        return datetime.strptime(s, "%y/%m/%d %H:%M:%S")
    except Exception:
        return None

# ---------------- Spark Session ----------------
def create_spark_session(master_url):
    spark = (
        SparkSession.builder
        .appName("Problem2_ClusterUsage_Cluster")
        .master(master_url)
        .config("spark.executor.memory", "6g")
        .config("spark.driver.memory", "6g")
        .config("spark.executor.cores", "2")
        .config("spark.cores.max", "6")
        # ✅ S3 
        .config("spark.jars.packages",
                "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    logger.info("✅ Spark session created successfully.")
    return spark

# ---------------- Main Analysis ----------------
def analyze_cluster_usage(spark, bucket, net_id):
    data_path = f"s3a://{net_id}-assignment-spark-cluster-logs/data/"
    output_prefix = f"s3a://{net_id}-assignment-spark-cluster-logs/output"
    local_out = "data/output"
    os.makedirs(local_out, exist_ok=True)

    logger.info(f"📂 Reading logs from {data_path}")
    df_raw = spark.read.option("recursiveFileLookup", "true").text(data_path)
    df_files = df_raw.withColumn("file_path", input_file_name())


    df_times = df_files.withColumn("time_str", regexp_extract("value", time_pattern, 1))
    df_times = df_times.filter(df_times.time_str != "")

    # application_id, cluster_id
    df_times = (
        df_times.withColumn("application_id",
                            regexp_extract("file_path", r"(application_\d+_\d+)", 1))
                .withColumn("cluster_id",
                            regexp_extract("file_path", r"application_(\d+)_\d+", 1))
    )
    df_times.createOrReplaceTempView("log_times")

    logger.info("⚙️ Aggregating start and end times per application...")
    df_app = spark.sql("""
        SELECT
            cluster_id,
            application_id,
            min(time_str) AS start_time_str,
            max(time_str) AS end_time_str
        FROM log_times
        WHERE cluster_id != '' AND application_id != ''
        GROUP BY cluster_id, application_id
    """)

    df_app_local = df_app.toPandas()
    df_app_local["start_time"] = df_app_local["start_time_str"].apply(parse_datetime)
    df_app_local["end_time"] = df_app_local["end_time_str"].apply(parse_datetime)
    df_app_local["app_number"] = df_app_local["application_id"].apply(lambda x: x.split("_")[-1])

    df_app_local = df_app_local[["cluster_id", "application_id", "app_number", "start_time", "end_time"]]
    timeline_path = f"{local_out}/problem2_timeline.csv"
    df_app_local.to_csv(timeline_path, index=False)
    logger.info(f"✅ Saved timeline to {timeline_path}")

    # ---------------- Cluster-level summary ----------------
    df_summary = (
        df_app_local.groupby("cluster_id")
        .agg(num_applications=("application_id", "count"),
             cluster_first_app=("start_time", "min"),
             cluster_last_app=("end_time", "max"))
        .reset_index()
    )

    summary_path = f"{local_out}/problem2_cluster_summary.csv"
    df_summary.to_csv(summary_path, index=False)
    logger.info(f"✅ Saved summary to {summary_path}")

    # ---------------- Stats file ----------------
    stats_text = f"""
Total unique clusters: {df_summary.shape[0]}
Total applications: {len(df_app_local)}
Average applications per cluster: {df_summary.num_applications.mean():.2f}

Most heavily used clusters:
{df_summary.sort_values('num_applications', ascending=False).head(6).to_string(index=False)}
"""
    stats_path = f"{local_out}/problem2_stats.txt"
    with open(stats_path, "w") as f:
        f.write(stats_text)
    logger.info(f"✅ Saved stats to {stats_path}")

    # ---------------- Visualization ----------------
    plt.figure(figsize=(10, 5))
    ax = sns.barplot(
        data=df_summary.sort_values("num_applications", ascending=False),
        x="cluster_id", y="num_applications", palette="tab10"
    )
    for p in ax.patches:
        ax.annotate(f"{int(p.get_height())}",
                    (p.get_x() + p.get_width() / 2., p.get_height()),
                    ha="center", va="bottom", xytext=(0, 3),
                    textcoords="offset points", fontsize=9)
    plt.title("Applications per Cluster")
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()
    plt.savefig(f"{local_out}/problem2_bar_chart.png", dpi=150)
    plt.close()

    largest_cluster = df_summary.sort_values("num_applications", ascending=False).iloc[0]["cluster_id"]
    df_largest = df_app_local[df_app_local["cluster_id"] == largest_cluster].copy()
    df_largest["duration_min"] = (df_largest["end_time"] - df_largest["start_time"]).dt.total_seconds() / 60.0

    plt.figure(figsize=(10, 5))
    sns.histplot(df_largest["duration_min"], kde=True, log_scale=True)
    plt.title(f"Job Duration Distribution for Cluster {largest_cluster} (n={len(df_largest)})")
    plt.xlabel("Duration (minutes, log scale)")
    plt.ylabel("Count")
    plt.tight_layout()
    plt.savefig(f"{local_out}/problem2_density_plot.png", dpi=150)
    plt.close()
    logger.info("✅ Plots saved successfully.")

    # ---------------- Upload to S3 ----------------
    os.system(f"aws s3 cp {local_out}/ s3://{net_id}-assignment-spark-cluster-logs/output/ --recursive --exclude '*.py'")

    logger.info("🎉 All outputs uploaded to S3 successfully.")
    logger.info(stats_text)

# ---------------- Main ----------------
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("master", nargs="?", help="spark://<MASTER_PRIVATE_IP>:7077")
    parser.add_argument("--net-id", required=False)
    parser.add_argument("--skip-spark", action="store_true")
    args = parser.parse_args()

    if args.skip_spark:
        logger.info("⚡ Skip-Spark mode: using existing CSVs to regenerate plots.")
        return  

    if not args.master or not args.net_id:
        print("Usage: python problem2.py spark://<MASTER_PRIVATE_IP>:7077 --net-id YOUR_NET_ID")
        sys.exit(1)

    spark = create_spark_session(args.master)
    analyze_cluster_usage(spark, f"s3://{args.net_id}-assignment-spark-cluster-logs", args.net_id)
    spark.stop()

if __name__ == "__main__":
    main()
