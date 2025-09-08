import requests
from utils.logger_config import logger
import os
from dotenv import load_dotenv
import pandas as pd
from utils.utilities import get_minio_client, upload_csv, upload_parquet
load_dotenv()
API_KEY = os.getenv("CENSUS_API_KEY")

base_url = "https://api.census.gov/data/timeseries/intltrade"
data_type = "porths"


def get_monthly_port_data(year, month, trade_type):
    endpoint = "exports" if trade_type == "export" else "imports"
    url = '/'.join([base_url, endpoint, data_type])
    try:
        monthly_dataframe = pd.DataFrame()
        commodity_param = "E_COMMODITY" if trade_type == "export" else "I_COMMODITY"
        get_columns = (
            "PORT,CTY_CODE,CTY_NAME,E_COMMODITY,E_COMMODITY_SDESC,ALL_VAL_MO,LAST_UPDATE,AIR_VAL_MO,AIR_WGT_MO,VES_VAL_MO,VES_WGT_MO"
            if trade_type == "export"
            else
            "PORT,CTY_CODE,CTY_NAME,I_COMMODITY,I_COMMODITY_SDESC,GEN_VAL_MO,LAST_UPDATE,AIR_VAL_MO,AIR_WGT_MO,VES_VAL_MO,VES_WGT_MO"
        )
        for commodity in ["1*","2*", "3*", "4*","5*","6*", "7*", "8*", "9*"]:
            params = {
                "YEAR": str(year),
                "MONTH": str(month).zfill(2),
                "get": get_columns,
                "SUMMARY_LVL": "DET",
                "COMM_LVL": "HS6",
                commodity_param: commodity,
                "key": API_KEY
            }
            response = requests.get(url, params=params)
            response.raise_for_status()
            commodity_data = response.json()
            rows = commodity_data[1:]
            columns = commodity_data[0]
            commodity_dataframe = pd.DataFrame(rows, columns=columns)
            commodity_dataframe = commodity_dataframe.iloc[:, :-1]
            logger.info(f"Data fetched successfully for commodity {commodity} ({trade_type})")
            monthly_dataframe = pd.concat([monthly_dataframe, commodity_dataframe], ignore_index=True) if not monthly_dataframe.empty else commodity_dataframe
        logger.info(f"Data for {year}-{str(month).zfill(2)} ({trade_type}) fetched successfully with {len(monthly_dataframe)} records")
        return monthly_dataframe
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching data: {e}")
        return None

def ingest_all_data(minio_client):
    trade_types = ["export", "import"]
    for trade_type in trade_types:
        for year in range(2015, 2026):
            annual_data = pd.DataFrame()
            for month in range(1, 13):
                monthly_data = get_monthly_port_data(year, month, trade_type)
                if monthly_data is not None:
                    annual_data = pd.concat([annual_data, monthly_data], ignore_index=True) if not annual_data.empty else monthly_data
            if annual_data.empty:
                logger.warning(f"No data for the year {year}")
                return
            logger.info(f"Data for {year} ingested successfully")
            upload_parquet(minio_client, os.getenv("MINIO_BUCKET_NAME"), annual_data, f"trade_data_port/total_{trade_type}_value_by_port_{year}.parquet")
    return None

def main():
    minio_client = get_minio_client(
        os.getenv("MINIO_EXTERNAL_URL"),
        os.getenv("MINIO_ACCESS_KEY"),
        os.getenv("MINIO_SECRET_KEY"),
        )
    ingest_all_data(minio_client)
    logger.info("Data uploaded to Minio")
    
    
    

if __name__ == "__main__":
    main()


