import subprocess
import sys
from pathlib import Path

from prefect import flow, task

parent_dir = Path(__file__).resolve().parent.parent
sys.path.append(str(parent_dir))
from utils.logger_config import logger

@task(retries=3, retry_delay_seconds=60)
def run_annual_wits():
    try:
        subprocess.run(
            ["python", "src/ingest_annual_wits.py"],
            check=True,
        )
        logger.info("Annual WITS Data ingestion completed successfully.")
    except subprocess.CalledProcessError as e:
        logger.info(f"WITS Data ingestion failed: {e}")
        raise
    
@task(retries=3, retry_delay_seconds=60)
def run_monthly_data():
    try:
        subprocess.run(
            ["python", "src/ingest_monthly_data.py"],
            check=True,
        )
        logger.info("Total Export Data ingestion completed successfully.")
    except subprocess.CalledProcessError as e:
        logger.info(f"Total Export Data ingestion failed: {e}")
        raise

@task(retries=3, retry_delay_seconds=60)
def run_tariff_cleanup():
    try:
        subprocess.run(
            ["python", "src/latest_tariff.py"],
            check=True,
        )
        logger.info("Tariff Data cleanup completed successfully.")
    except subprocess.CalledProcessError as e:
        logger.info(f"Tariff Data cleanup failed: {e}")
        raise

project_dir = "/workspace/dbt/dbt_trade/"
profiles_dir = "/workspace/dbt/dbt_trade/"

@task(retries=3, retry_delay_seconds=60)
def run_dbt_raw():
    try:
        subprocess.run(
            [
                "dbt",
                "run",
                "--select",
                "models/raw/",
                "--project-dir",
                project_dir,
                "--profiles-dir",
                profiles_dir,
            ],
            check=True,
        )
        logger.info("DBT Raw run completed successfully.")
    except subprocess.CalledProcessError as e:
        logger.info(f"DBT Raw run failed: {e}")
        raise

@task(retries=3, retry_delay_seconds=60)
def run_dbt_staging():
    try:
        subprocess.run(
            [
                "dbt",
                "run",
                "--select",
                "models/staged/",
                "--project-dir",
                project_dir,
                "--profiles-dir",
                profiles_dir,
            ],
            check=True,
        )
        logger.info("DBT Staged run completed successfully.")
    except subprocess.CalledProcessError as e:
        logger.info(f"DBT Staged run failed: {e}")
        raise



@flow(name="trade_data")
def main_flow():
    # run_annual_wits()
    # run_monthly_data()
    # run_tariff_cleanup()
    run_dbt_raw()
    run_dbt_staging()


if __name__ == "__main__":
    main_flow()
