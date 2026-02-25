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

spark.stop()
