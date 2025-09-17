import io
import sys
from pathlib import Path

from minio import Minio

parent_dir = Path(__file__).resolve().parent.parent
sys.path.append(str(parent_dir))
import pandas as pd

from utils.connection import DremioConnection
from utils.logger_config import logger


def get_dremio_connection(user, password, host, port):
    try:
        dremio_conn = DremioConnection(
            user=user, password=password, host=host, port=port
        )
        logger.info("DremioConnection instance created successfully.")
        return dremio_conn
    except Exception as e:
        logger.error(f"Error in connect: {e}")
        raise e


def query_table(con: DremioConnection, query: str):
    try:
        dremio_client = con.connect()
        logger.info("Connected to Dremio successfully.")

        logger.info("SQL query loaded successfully.")
        results = con.query(query, dremio_client)
        data = results.read_all().to_pandas()
        logger.info(f"Query executed and data {data.head(0)}fetched successfully.")
        return data
    except Exception as e:
        logger.error(f"Error in main: {e}")
        raise e


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
    logger.info(
        f"DataFrame uploaded to MinIO bucket {bucket_name} as {full_object_name}"
    )


def get_csv_from_lake(minio_client, bucket_name, object_name):
    try:
        response = minio_client.get_object(bucket_name, object_name)
        data = pd.read_csv(io.BytesIO(response.read()), encoding="latin1")
        logger.info(
            f"CSV data fetched from MinIO bucket {bucket_name} as {object_name}"
        )
        return data
    except Exception as e:
        logger.error(f"Failed to fetch CSV data from MinIO: {e}")
        raise
