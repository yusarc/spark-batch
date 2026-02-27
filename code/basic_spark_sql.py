import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    IntegerType,
)
from pyspark.sql.functions import col, to_date, count


"""
PySpark batch job to process NYC FHVHV trips on Dataproc.

Reads CSV from GCS, writes transformed Parquet back to GCS,
and loads daily trip counts into a BigQuery table.
"""


def create_spark_session(app_name: str = "fhvhv-batch") -> SparkSession:
    spark = (
        SparkSession.builder
        .appName(app_name)
        # Dataproc config is mostly handled by the cluster configuration.
        .getOrCreate()
    )
    return spark


def read_csv_with_schema(spark: SparkSession, input_path: str):
    schema = StructType(
        [
            StructField("hvfhs_license_num", StringType(), True),
            StructField("dispatching_base_num", StringType(), True),
            StructField("pickup_datetime", TimestampType(), True),
            StructField("dropoff_datetime", TimestampType(), True),
            StructField("PULocationID", IntegerType(), True),
            StructField("DOLocationID", IntegerType(), True),
            StructField("SR_Flag", StringType(), True),
        ]
    )

    df = (
        spark.read
        .option("header", "true")
        .schema(schema)
        .csv(input_path)
    )
    return df


def transform_trips(df):
    transformed_df = (
        df.withColumn("pickup_date", to_date(col("pickup_datetime")))
        .withColumn("dropoff_date", to_date(col("dropoff_datetime")))
        .withColumnRenamed("dispatching_base_num", "base_id")
        .select(
            "base_id",
            "pickup_date",
            "dropoff_date",
            "PULocationID",
            "DOLocationID",
        )
    )
    return transformed_df


def aggregate_daily_trips(df):
    daily_trips_df = (
        df.groupBy("pickup_date")
        .agg(count("*").alias("trips"))
        .orderBy("pickup_date")
    )
    return daily_trips_df


def top_pickup_locations(df, top_n: int = 10):
    top_pu_df = (
        df.groupBy("PULocationID")
        .agg(count("*").alias("trips"))
        .orderBy(col("trips").desc())
        .limit(top_n)
    )
    return top_pu_df


def write_parquet(df, output_path: str):
    (
        df.write
        .mode("overwrite")
        .parquet(output_path)
    )


def write_to_bigquery(df, table_fqn: str):
    """
    Write a DataFrame to a BigQuery table using the Spark BigQuery connector.

    Example:
        table_fqn = "spark-batch-project.ny_taxi.trips_per_day"
    """
    (
        df.write
        .format("bigquery")
        .option("table", table_fqn)
        .option("writeMethod", "direct")
        .mode("overwrite")
        .save()
    )


def main(input_path: str, output_path: str):
    spark = create_spark_session()

    print(f"Reading CSV from: {input_path}")
    df = read_csv_with_schema(spark, input_path)

    print("Schema with explicit types:")
    df.printSchema()

    transformed_df = transform_trips(df)

    print("Sample transformed rows:")
    transformed_df.show(20, truncate=False)

    daily_trips_df = aggregate_daily_trips(transformed_df)

    print("Trips per pickup_date:")
    daily_trips_df.show(10, truncate=False)

    top_pu_df = top_pickup_locations(transformed_df)

    print("Top 10 pickup locations by number of trips:")
    top_pu_df.show(10, truncate=False)

    print(f"Writing transformed data to Parquet at: {output_path}")
    write_parquet(transformed_df, output_path)

    print(
        "Writing daily trips aggregation to BigQuery table: "
        "spark-batch-project.ny_taxi.trips_per_day"
    )
    write_to_bigquery(
        daily_trips_df,
        "spark-batch-project.ny_taxi.trips_per_day",
    )

    print("Job completed successfully.")
    spark.stop()


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python basic_spark_sql.py <input_path> <output_path>")
        sys.exit(1)

    input_path_arg = sys.argv[1]
    output_path_arg = sys.argv[2]

    main(input_path_arg, output_path_arg)