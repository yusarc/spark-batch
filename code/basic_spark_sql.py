from pyspark.sql import SparkSession
from pyspark.sql import types, functions as F


# SparkSession
spark = SparkSession.builder \
    .master("local[*]") \
    .appName("fhvhv-first-look") \
    .getOrCreate()

print(f"Spark version: {spark.version}")

# 1) Şemayı tanımla
schema = types.StructType([
    types.StructField("hvfhs_license_num", types.StringType(), True),
    types.StructField("dispatching_base_num", types.StringType(), True),
    types.StructField("pickup_datetime", types.TimestampType(), True),
    types.StructField("dropoff_datetime", types.TimestampType(), True),
    types.StructField("PULocationID", types.IntegerType(), True),
    types.StructField("DOLocationID", types.IntegerType(), True),
    types.StructField("SR_Flag", types.StringType(), True),
])

# 2) CSV'yi bu şema ile oku
df = spark.read \
    .option("header", "true") \
    .schema(schema) \
    .csv("data/fhvhv_tripdata_2021-01.csv")

print("Schema with explicit types:")
df.printSchema()

# 3) Repartition + Parquet'e yaz
df = df.repartition(24)
output_path = "data/fhvhv/2021/01/"
df.write.mode("overwrite").parquet(output_path)

# 4) Parquet'ten tekrar oku
df = spark.read.parquet(output_path)

print("Schema after reading from Parquet:")
df.printSchema()

# 5) UDF yerine pure Spark fonksiyonları ile base_id üret
num = F.substring("dispatching_base_num", 2, 10).cast("int")

base_id_col = (
    F.when(num % 7 == 0, F.concat(F.lit("s/"), F.format_string("%03x", num)))
     .when(num % 3 == 0, F.concat(F.lit("a/"), F.format_string("%03x", num)))
     .otherwise(F.concat(F.lit("e/"), F.format_string("%03x", num)))
)

# 6) Yeni kolonlar: pickup_date, dropoff_date, base_id
df_transformed = df \
    .withColumn("pickup_date", F.to_date("pickup_datetime")) \
    .withColumn("dropoff_date", F.to_date("dropoff_datetime")) \
    .withColumn("base_id", base_id_col) \
    .select("base_id", "pickup_date", "dropoff_date", "PULocationID", "DOLocationID")

print("Sample transformed rows:")
df_transformed.show(20, truncate=False)

# 7) 5.3.2 – DataFrame aggregation örnekleri

# 7.1 Gün bazında trip sayısı
print("Trips per pickup_date:")
df_trips_per_day = (
    df_transformed
        .groupBy("pickup_date")
        .agg(F.count("*").alias("trips"))
        .orderBy("pickup_date")
)
df_trips_per_day.show(10, truncate=False)

# 7.2 En çok kullanılan 10 pickup location
print("Top 10 pickup locations by number of trips:")
df_top_pu = (
    df_transformed
        .groupBy("PULocationID")
        .agg(F.count("*").alias("trips"))
        .orderBy(F.desc("trips"))
        .limit(10)
)
df_top_pu.show(truncate=False)

# DataFrame'den temp view oluştur
df_transformed.createOrReplaceTempView("fhvhv_trips")

print("Trips per pickup_date (SQL):")
df_trips_per_day_sql = spark.sql("""
    SELECT
        pickup_date,
        COUNT(*) AS trips
    FROM fhvhv_trips
    GROUP BY pickup_date
    ORDER BY pickup_date
""")
df_trips_per_day_sql.show(10, truncate=False)

print("Top 10 pickup locations by number of trips (SQL):")
df_top_pu_sql = spark.sql("""
    SELECT
        PULocationID,
        COUNT(*) AS trips
    FROM fhvhv_trips
    GROUP BY PULocationID
    ORDER BY trips DESC
    LIMIT 10
""")
df_top_pu_sql.show(truncate=False)

spark.stop()