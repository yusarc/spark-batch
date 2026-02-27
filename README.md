# NYC FHVHV Trips ETL on GCP (Dataproc + PySpark + BigQuery)

This project implements an end‑to‑end batch ETL pipeline on Google Cloud
for the NYC FHVHV (For‑Hire Vehicle High Volume) trips dataset.

The pipeline reads raw CSV data from Google Cloud Storage (GCS),
processes it with PySpark on a Dataproc cluster, writes transformed
Parquet files back to GCS, and loads daily trip counts into a
BigQuery table.

## Technologies

- Google Cloud Storage (GCS) for raw and processed data
- Dataproc cluster for running PySpark batch jobs
- PySpark for transformations and aggregations
- BigQuery for analytical storage and SQL querying
- Git & GitHub for version control

## Dataset and Objective

The pipeline uses the NYC TLC FHVHV (For‑Hire Vehicle High Volume)
trips dataset for January 2021 (`fhvhv_tripdata_2021-01.csv`).

**Objective**

- Ingest the raw CSV file from GCS.
- Clean and normalize timestamps and location columns.
- Produce:
  - A curated trips table in Parquet format on GCS.
  - A BigQuery table with the number of trips per `pickup_date`
    (`ny_taxi.trips_per_day`).

## Project Structure

```text
.
├── code
│   └── basic_spark_sql.py      # PySpark batch job (GCS -> GCS + BigQuery)
├── data
│   ├── fhvhv_tripdata_2021-01.csv
│   └── fhvhv_tripdata_2021-01.csv.gz
├── notebooks
│   └── ... (exploration / scratch work)
├── .gitignore
└── README.md
```


## Architecture / Pipeline Overview

1. **Storage (GCS)**  
   - Raw file:  
     `gs://spark-batch-data/raw/fhvhv_tripdata_2021-01.csv`  
   - Transformed Parquet output:  
     `gs://spark-batch-data/fhvhv/2021/01/`

2. **Compute (Dataproc + PySpark)**  
   - Dataproc cluster: `spark-batch-cluster` in `europe-west3`  
   - PySpark job: `code/basic_spark_sql.py` submitted as a Dataproc job.

3. **Analytics (BigQuery)**  
   - Dataset: `spark-batch-project.ny_taxi`  
   - Table: `spark-batch-project.ny_taxi.trips_per_day`

High‑level flow:

GCS (raw CSV)
    -> Dataproc (PySpark transformations)
        -> GCS (curated Parquet)
        -> BigQuery (daily trips aggregate table)



## PySpark Job (`basic_spark_sql.py`)

The main steps implemented in `code/basic_spark_sql.py` are:

1. **Create Spark session**

   Uses `SparkSession.builder.appName("fhvhv-batch").getOrCreate()`.

2. **Read CSV with an explicit schema**

   ```python
   schema = StructType([
       StructField("hvfhs_license_num", StringType(), True),
       StructField("dispatching_base_num", StringType(), True),
       StructField("pickup_datetime", TimestampType(), True),
       StructField("dropoff_datetime", TimestampType(), True),
       StructField("PULocationID", IntegerType(), True),
       StructField("DOLocationID", IntegerType(), True),
       StructField("SR_Flag", StringType(), True),
   ])
   ```
## Transform trips

Convert timestamps to dates:

```python
pickup_date = to_date(pickup_datetime)

dropoff_date = to_date(dropoff_datetime)

```

Rename dispatching_base_num -> base_id.

Select only relevant columns:
base_id, pickup_date, dropoff_date, PULocationID, DOLocationID.

Aggregations

Daily trips:

```python
daily_trips_df = (
    transformed_df.groupBy("pickup_date")
    .agg(count("*").alias("trips"))
    .orderBy("pickup_date")
)
```
Top pickup locations (top 10) by trip count.

Outputs

Write transformed trips to Parquet on GCS:

``` python
write_parquet(transformed_df, output_path)
Write daily trips aggregation to BigQuery:
```
``` python
write_to_bigquery(
    daily_trips_df,
    "spark-batch-project.ny_taxi.trips_per_day",
)
```
The script expects two arguments:

```python
python basic_spark_sql.py <input_path> <output_path>
On Dataproc, these are GCS paths.

```

## Prerequisites

- Google Cloud project: `spark-batch-project`
- GCS bucket for data: `spark-batch-data`
- Dataproc cluster:
  - Name: `spark-batch-cluster`
  - Region: `europe-west3`
  - Image: Debian‑based Dataproc image with Spark
- BigQuery dataset:
  - `ny_taxi` created in project `spark-batch-project`


## How to Run the Pipeline

### 1. Upload the PySpark script to GCS

From the project root:

```bash
gsutil cp code/basic_spark_sql.py gs://spark-batch-data/code/basic_spark_sql.py
```

### 2.Submit the job to Dataproc

```bash
gcloud dataproc jobs submit pyspark gs://spark-batch-data/code/basic_spark_sql.py \
  --cluster=spark-batch-cluster \
  --region=europe-west3 \
  -- \
  gs://spark-batch-data/raw/fhvhv_tripdata_2021-01.csv \
  gs://spark-batch-data/fhvhv/2021/01/
  ```

Where:

The first argument is the input CSV on GCS.

The second argument is the output path for Parquet files.

### 3. Monitor the job

In the GCP Console, go to: Dataproc → Jobs.

Open the job ID and inspect the Driver output to see:

Schema and sample rows

Daily trip counts

Top pickup locations

## Validating the Result in BigQuery

After the job finishes successfully, the table
`spark-batch-project.ny_taxi.trips_per_day` should be populated.

Example query:

```sql
SELECT *
FROM `spark-batch-project.ny_taxi.trips_per_day`
ORDER BY pickup_date
LIMIT 5;
```
Expected sample output (counts may differ if the source file changes):

text```
pickup_date   trips
-----------   -----
2021-01-01    406887
2021-01-02    332418
2021-01-03    299929
2021-01-04    320488
2021-01-05    336686



## Possible Extensions

Some ideas for future improvements:

- Write additional aggregations to BigQuery (e.g. top pickup locations table).
- Partition the BigQuery table by `pickup_date` and cluster by `PULocationID`.
- Orchestrate the job with Cloud Composer, Workflows, or Cloud Scheduler.
- Parameterize the script for different months or datasets.
- Add unit tests for the transformation logic (e.g. with `pytest`).

## Notes

- This project follows a simple, explicit style to make each step of the
  data pipeline transparent (from raw ingestion to analytical storage).
- The same pattern can be reused for other large CSV‑based datasets:
  land them in GCS, process with Spark on Dataproc, and expose aggregates
  through BigQuery.
