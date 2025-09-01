from pyspark.sql import SparkSession
from logger_config import logger
import pyspark
import os
from dotenv import load_dotenv
load_dotenv()

# NESSIE_URI = os.environ.get("NESSIE_URI")
# NESSIE_URI = "http://172.19.0.4:19120/api/v1"
NESSIE_URI = "http://localhost:19120/api/v1"
WAREHOUSE = os.environ.get("WAREHOUSE")  
AWS_ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY")
AWS_SECRET_KEY = os.environ.get("AWS_SECRET_KEY")
# AWS_S3_ENDPOINT = os.environ.get("AWS_S3_ENDPOINT")
AWS_S3_ENDPOINT = "http://localhost:9000"
conf = (
    pyspark.SparkConf()
        .setAppName("trade_pipeline")
        .set(
            "spark.jars.packages",
            ",".join([
                "org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.3.1",
                "org.projectnessie.nessie-integrations:nessie-spark-extensions-3.3_2.12:0.67.0",
                "software.amazon.awssdk:bundle:2.17.178",
                "software.amazon.awssdk:url-connection-client:2.17.178"
            ])
        )
        .set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSparkSessionExtensions")
        .set("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog")
        .set("spark.sql.catalog.nessie.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog")
        .set("spark.sql.catalog.nessie.uri", NESSIE_URI)
        .set("spark.sql.catalog.nessie.ref", "main")
        .set("spark.sql.catalog.nessie.authentication.type", "NONE")
        .set("spark.sql.catalog.nessie.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .set("spark.sql.catalog.nessie.warehouse", WAREHOUSE)
        .set("spark.sql.catalog.nessie.s3.endpoint", AWS_S3_ENDPOINT)
        .set("spark.sql.catalog.nessie.s3.path-style-access", "true")
        .set("spark.sql.catalog.nessie.client.timeout", "30s")
        # S3 Credentials
        .set("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY)
        .set("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_KEY)
        .set("spark.sql.defaultCatalog", "nessie")
        .set("spark.sql.catalog.demo", "")
)

def get_spark_session():
    spark = SparkSession.builder \
        .config(conf=conf) \
        .getOrCreate()
    logger.info("Spark Session with Nessie Catalog initialized.")
    return spark

def main():
    # Initialize SparkSession
    spark = get_spark_session()
    spark.sql("SHOW TABLES IN nessie.default").show()
    spark.stop()
    logger.info("Spark Session stopped.")

if __name__ == "__main__":
    main()