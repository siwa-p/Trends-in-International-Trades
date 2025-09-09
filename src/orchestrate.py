import subprocess
import sys
from pathlib import Path
from prefect import flow, task

parent_dir = Path(__file__).resolve().parent.parent
sys.path.append(str(parent_dir))
from utils.logger_config import logger

project_dir = "/home/prahald/Documents/Data Engineering Bootcamp/capstone/dbt/dbt_trade/"
profiles_dir = "/home/prahald/Documents/Data Engineering Bootcamp/capstone/dbt/dbt_trade/"

@task(retries=3, retry_delay_seconds=60)
def run_dbt_bronze():
    try:
        subprocess.run(
            [
                "dbt",
                "run",
                "--select",
                "models/bronze/",
                "--project-dir",
                project_dir,
                "--profiles-dir",
                profiles_dir,
            ],
            check=True,
        )
        logger.info("DBT Bronze run completed successfully.")
    except subprocess.CalledProcessError as e:
        logger.info(f"DBT Bronze run failed: {e}")
        raise



@task(retries=3, retry_delay_seconds=60)
def run_dbt_silver():
    try:
        subprocess.run(
            [
                "dbt",
                "run",
                "--select",
                "models/silver/",
                "--project-dir",
                project_dir,
                "--profiles-dir",
                profiles_dir,
            ],
            check=True,
        )
    except subprocess.CalledProcessError as e:
        logger.info(f"DBT Silver run failed: {e}")
        raise


@task(retries=3, retry_delay_seconds=60)
def run_dbt_gold():
    try:
        subprocess.run(
            [
                "dbt",
                "run",
                "--select",
                "models/gold/",
                "--project-dir",
                project_dir,
                "--profiles-dir",
                profiles_dir,
            ],
            check=True,
        )
        logger.info("DBT Gold run completed successfully.")
    except subprocess.CalledProcessError as e:
        logger.info(f"DBT Gold run failed: {e}")
        raise




@flow(name="trade_data")
def main_flow():
    run_dbt_bronze()
    # run_dbt_silver()
    # run_dbt_gold()


if __name__ == "__main__":
    main_flow()
