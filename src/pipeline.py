from __future__ import annotations

from pathlib import Path
from typing import Iterable

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


TURBINE_SCHEMA = StructType(
    [
        StructField("timestamp", TimestampType(), True),
        StructField("turbine_id", IntegerType(), True),
        StructField("wind_speed", DoubleType(), True),
        StructField("wind_direction", IntegerType(), True),
        StructField("power_output", DoubleType(), True),
    ]
)

MEASUREMENT_COLUMNS = ("wind_speed", "wind_direction", "power_output")
IQR_COLUMNS = ("wind_speed", "power_output")


def create_spark(app_name: str = "wind-turbine-pipeline") -> SparkSession:
    """Create a local Spark session with a persistent SQL warehouse."""
    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .enableHiveSupport()
        .getOrCreate()
    )


def ingest_csvs(spark: SparkSession, input_path: str) -> DataFrame:
    """Read all turbine CSV files matching the supplied path/pattern."""
    return (
        spark.read.option("header", True)
        .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
        .schema(TURBINE_SCHEMA)
        .csv(input_path)
    )


def _mark_invalid_values(df: DataFrame) -> DataFrame:
    """Convert physically invalid sensor values to null before imputation."""
    return (
        df.withColumn(
            "wind_speed",
            F.when(F.col("wind_speed") >= 0, F.col("wind_speed")),
        )
        .withColumn(
            "wind_direction",
            F.when(
                (F.col("wind_direction") >= 0) & (F.col("wind_direction") < 360),
                F.col("wind_direction"),
            ),
        )
        .withColumn(
            "power_output",
            F.when(F.col("power_output") >= 0, F.col("power_output")),
        )
    )


def _mark_iqr_outliers_as_null(
    df: DataFrame, columns: Iterable[str] = IQR_COLUMNS
) -> DataFrame:
    """
    Mark statistical sensor outliers as null using 1.5 * IQR bounds per turbine.

    Wind direction is deliberately excluded because it is circular (0 and 359 degrees
    are adjacent rather than extreme values).
    """
    result = df

    for column in columns:
        bounds = result.groupBy("turbine_id").agg(
            F.expr(f"percentile_approx({column}, 0.25, 10000)").alias("q1"),
            F.expr(f"percentile_approx({column}, 0.75, 10000)").alias("q3"),
        )
        bounds = (
            bounds.withColumn("iqr", F.col("q3") - F.col("q1"))
            .withColumn("lower_bound", F.col("q1") - 1.5 * F.col("iqr"))
            .withColumn("upper_bound", F.col("q3") + 1.5 * F.col("iqr"))
            .select("turbine_id", "lower_bound", "upper_bound")
        )

        result = (
            result.join(bounds, on="turbine_id", how="left")
            .withColumn(
                column,
                F.when(
                    F.col(column).between(
                        F.col("lower_bound"), F.col("upper_bound")
                    ),
                    F.col(column),
                ),
            )
            .drop("lower_bound", "upper_bound")
        )

    return result


def _median_impute_by_turbine(df: DataFrame) -> DataFrame:
    """Impute missing numeric sensor values with the per-turbine median."""
    medians = df.groupBy("turbine_id").agg(
        *[
            F.expr(f"percentile_approx({column}, 0.5, 10000)").alias(
                f"{column}_median"
            )
            for column in MEASUREMENT_COLUMNS
        ]
    )

    result = df.join(medians, on="turbine_id", how="left")
    for column in MEASUREMENT_COLUMNS:
        result = result.withColumn(
            column, F.coalesce(F.col(column), F.col(f"{column}_median"))
        ).drop(f"{column}_median")

    return result


def clean_data(df: DataFrame) -> DataFrame:
    """
    Clean raw turbine readings.

    - Drop records without a timestamp or turbine identifier.
    - Remove duplicate readings for the same turbine/timestamp.
    - Treat invalid physical values and IQR outliers as missing.
    - Impute missing measurements with the turbine-level median.
    """
    cleaned = (
        df.dropna(subset=["timestamp", "turbine_id"])
        .dropDuplicates(["turbine_id", "timestamp"])
    )
    cleaned = _mark_invalid_values(cleaned)
    cleaned = _mark_iqr_outliers_as_null(cleaned)
    cleaned = _median_impute_by_turbine(cleaned)
    return cleaned


def calculate_daily_summary(df: DataFrame, expected_readings: int = 24) -> DataFrame:
    """Calculate 24-hour summary statistics and report sensor completeness."""
    return (
        df.withColumn("date", F.to_date("timestamp"))
        .groupBy("date", "turbine_id")
        .agg(
            F.min("power_output").alias("min_power_output"),
            F.max("power_output").alias("max_power_output"),
            F.avg("power_output").alias("avg_power_output"),
            F.stddev_samp("power_output").alias("stddev_power_output"),
            F.count("power_output").alias("reading_count"),
        )
        .withColumn(
            "missing_readings",
            F.greatest(
                F.lit(0), F.lit(expected_readings) - F.col("reading_count")
            ),
        )
        .withColumn("is_complete_period", F.col("reading_count") >= expected_readings)
        .orderBy("date", "turbine_id")
    )


def identify_anomalies(summary_df: DataFrame) -> DataFrame:
    """
    Flag turbine/day averages outside fleet mean +/- 2 standard deviations.

    This interprets the brief's anomaly requirement at turbine level: each turbine's
    24-hour average is compared with the distribution of all turbine averages for
    the same day.
    """
    daily_fleet = Window.partitionBy("date")

    enriched = (
        summary_df.withColumn(
            "fleet_mean_power", F.avg("avg_power_output").over(daily_fleet)
        )
        .withColumn(
            "fleet_stddev_power", F.stddev_samp("avg_power_output").over(daily_fleet)
        )
        .withColumn(
            "lower_anomaly_bound",
            F.col("fleet_mean_power") - 2 * F.col("fleet_stddev_power"),
        )
        .withColumn(
            "upper_anomaly_bound",
            F.col("fleet_mean_power") + 2 * F.col("fleet_stddev_power"),
        )
        .withColumn(
            "is_anomaly",
            F.when(F.col("fleet_stddev_power").isNull(), F.lit(False)).otherwise(
                (F.col("avg_power_output") < F.col("lower_anomaly_bound"))
                | (F.col("avg_power_output") > F.col("upper_anomaly_bound"))
            ),
        )
    )

    return enriched.orderBy("date", "turbine_id")


def store_results(
    spark: SparkSession,
    cleaned_df: DataFrame,
    summary_with_anomalies_df: DataFrame,
    database: str = "wind_turbine",
) -> None:
    """Persist POC outputs as Spark SQL managed tables backed by the local warehouse."""
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")

    cleaned_df.write.mode("overwrite").saveAsTable(f"{database}.cleaned_readings")
    summary_with_anomalies_df.write.mode("overwrite").saveAsTable(
        f"{database}.daily_summary"
    )
    summary_with_anomalies_df.filter(F.col("is_anomaly")).write.mode(
        "overwrite"
    ).saveAsTable(f"{database}.anomalies")


def run_pipeline(input_path: str, database: str = "wind_turbine") -> None:
    spark = create_spark()
    try:
        raw = ingest_csvs(spark, input_path)
        cleaned = clean_data(raw)
        summary = calculate_daily_summary(cleaned)
        summary_with_anomalies = identify_anomalies(summary)
        store_results(spark, cleaned, summary_with_anomalies, database=database)

        print(f"Raw records: {raw.count()}")
        print(f"Cleaned records: {cleaned.count()}")
        print(f"Summary rows: {summary_with_anomalies.count()}")
        print(
            "Anomaly rows: "
            f"{summary_with_anomalies.filter(F.col('is_anomaly')).count()}"
        )
        print(f"Results stored in Spark SQL database: {database}")
    finally:
        spark.stop()


if __name__ == "__main__":
    project_root = Path(__file__).resolve().parents[1]
    run_pipeline(str(project_root / "data" / "data_group_*.csv"))
