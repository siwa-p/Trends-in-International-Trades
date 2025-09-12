from minio import Minio
import io
import sys
from pathlib import Path
parent_dir = Path(__file__).resolve().parent.parent
sys.path.append(str(parent_dir))
from utils.logger_config import logger
import pandas as pd
import pyspark
from pyspark.sql import SparkSession

def create_spark_session():
    url = "http://localhost:19120/api/v2"
    full_path_to_warehouse = "s3://warehouse/"
    ref = "main"
    auth_type = "NONE"
    spark = SparkSession.builder \
        .config("spark.jars.packages",
                "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.2,"
                "org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.103.3,"
                "software.amazon.awssdk:bundle:2.20.158,"
                "org.apache.hadoop:hadoop-aws:3.3.4") \
            .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")\
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSparkSessionExtensions") \
            .config("spark.sql.catalog.nessie.uri", url) \
            .config("spark.sql.catalog.nessie.ref", ref) \
            .config("spark.sql.catalog.nessie.authentication.type", auth_type) \
            .config("spark.sql.catalog.nessie.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog") \
            .config("spark.sql.catalog.nessie.warehouse", full_path_to_warehouse) \
            .config("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog") \
            .config("spark.sql.catalog.nessie.s3.endpoint", "http://localhost:9000") \
            .config("spark.hadoop.fs.s3a.access.key", "admin") \
            .config("spark.hadoop.fs.s3a.secret.key", "password") \
            .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
            .config("spark.hadoop.fs.s3a.path.style.access", True) \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.sql.catalog.nessie.s3.access-key-id", "admin") \
            .config("spark.sql.catalog.nessie.s3.secret-access-key", "password") \
            .config("spark.sql.catalog.nessie.s3.path-style-access", True) \
            .config("spark.sql.catalog.nessie.s3.region", "us-east-1") \
            .getOrCreate()
    return spark


def get_minio_client(minio_url, minio_access_key, minio_secret_key):
    try:
        minio_client = Minio(
            minio_url,
            access_key=minio_access_key,
            secret_key=minio_secret_key,
            secure=False,
        )
        logger.info("Connected to MinIO successfully.")
        return minio_client
    except Exception as e:
        logger.error(f"Failed to connect to MinIO: {e}")
        raise

def upload_csv(minio_client, bucket_name, data, object_name):
    if not minio_client.bucket_exists(bucket_name):
        logger.info(f"Bucket {bucket_name} does not exist. Creating it.")
        minio_client.make_bucket(bucket_name)
    csv_buffer = io.StringIO()
    data.to_csv(csv_buffer, index=False)
    csv_bytes = csv_buffer.getvalue().encode("utf-8")
    minio_client.put_object(
        bucket_name,
        object_name,
        io.BytesIO(csv_bytes),
        length=len(csv_bytes),
        content_type="text/csv",
    )
    logger.info(f"Data uploaded to MinIO bucket {bucket_name} as {object_name}")
    
def upload_parquet(minio_client, bucket_name, dataframe, object_name):
    if not minio_client.bucket_exists(bucket_name):
        logger.info(f"Bucket {bucket_name} does not exist. Creating it.")
        minio_client.make_bucket(bucket_name)
    parquet_buffer = io.BytesIO()
    dataframe.to_parquet(parquet_buffer, index=False)
    parquet_buffer.seek(0)
    full_object_name = f"{object_name}"
    minio_client.put_object(
        bucket_name,
        full_object_name,
        parquet_buffer,
        length=parquet_buffer.getbuffer().nbytes,
        content_type="application/octet-stream",
    )
    logger.info(f"DataFrame uploaded to MinIO bucket {bucket_name} as {full_object_name}")
  
def get_csv_from_lake(minio_client, bucket_name, object_name):
    try:
        response = minio_client.get_object(bucket_name, object_name)
        data = pd.read_csv(io.BytesIO(response.read()), encoding='latin1')
        logger.info(f"CSV data fetched from MinIO bucket {bucket_name} as {object_name}")
        return data
    except Exception as e:
        logger.error(f"Failed to fetch CSV data from MinIO: {e}")
        raise
