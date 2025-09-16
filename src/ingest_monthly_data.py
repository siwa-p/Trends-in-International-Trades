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
DATA_TYPE_COLUMNS = {
    "porths": {
        "export": "PORT,PORT_NAME, CTY_CODE,CTY_NAME,E_COMMODITY,E_COMMODITY_SDESC,ALL_VAL_MO,LAST_UPDATE,AIR_VAL_MO,AIR_WGT_MO,VES_VAL_MO,VES_WGT_MO",
        "import": "PORT,PORT_NAME, CTY_CODE,CTY_NAME,I_COMMODITY,I_COMMODITY_SDESC,GEN_VAL_MO,LAST_UPDATE,AIR_VAL_MO,AIR_WGT_MO,VES_VAL_MO,VES_WGT_MO"
    },
    "statehs": {
        "export": "STATE,CTY_CODE,CTY_NAME,E_COMMODITY,E_COMMODITY_SDESC,ALL_VAL_MO,LAST_UPDATE,AIR_VAL_MO,AIR_WGT_MO,VES_VAL_MO,VES_WGT_MO",
        "import": "STATE,CTY_CODE,CTY_NAME,I_COMMODITY,I_COMMODITY_SDESC,GEN_VAL_MO,LAST_UPDATE,AIR_VAL_MO,AIR_WGT_MO,VES_VAL_MO,VES_WGT_MO"
    },
    "hs": {
        "export": "CTY_CODE,CTY_NAME,DISTRICT,DIST_NAME,E_COMMODITY,E_COMMODITY_SDESC,ALL_VAL_MO,ALL_VAL_YR,AIR_VAL_MO,AIR_VAL_YR,AIR_WGT_MO,AIR_WGT_YR,CC_MO,CC_YR,CNT_VAL_MO,CNT_VAL_YR,CNT_WGT_MO,CNT_WGT_YR,LAST_UPDATE,QTY_1_MO,QTY_1_MO_FLAG,QTY_1_YR,QTY_1_YR_FLAG,QTY_2_MO,QTY_2_MO_FLAG,QTY_2_YR,QTY_2_YR_FLAG,UNIT_QY1,UNIT_QY2,VES_VAL_MO,VES_VAL_YR,VES_WGT_MO,VES_WGT_YR",
        "import": "CTY_CODE,CTY_NAME,DISTRICT,DIST_NAME,I_COMMODITY,I_COMMODITY_SDESC,GEN_VAL_MO,GEN_VAL_YR,CON_VAL_MO,CON_VAL_YR,AIR_VAL_MO,AIR_VAL_YR,AIR_WGT_MO,AIR_WGT_YR,CC_MO,CC_YR,CNT_VAL_MO,CNT_VAL_YR,CNT_WGT_MO,CNT_WGT_YR,GEN_QY1_MO,GEN_QY1_MO_FLAG,GEN_QY1_YR,GEN_QY1_YR_FLAG,GEN_QY2_MO,GEN_QY2_MO_FLAG,GEN_QY2_YR,GEN_QY2_YR_FLAG,LAST_UPDATE,UNIT_QY1,UNIT_QY2,VES_VAL_MO,VES_VAL_YR,VES_WGT_MO,VES_WGT_YR"
    }
}
def get_monthly_port_data(year, month, trade_type, data_type):
    endpoint = "exports" if trade_type == "export" else "imports"
    url = '/'.join([base_url, endpoint, data_type])
    try:
        monthly_dataframe = pd.DataFrame()
        commodity_param = "E_COMMODITY" if trade_type == "export" else "I_COMMODITY"
        get_columns = DATA_TYPE_COLUMNS[data_type][trade_type]
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

def ingest_all_data(minio_client, data_type):
    trade_types = ["import"]
    for trade_type in trade_types:
        for year in range(2015, 2026):
            annual_data = pd.DataFrame()
            for month in range(1, 13):
                monthly_data = get_monthly_port_data(year, month, trade_type, data_type)
                if monthly_data is not None:
                    annual_data = pd.concat([annual_data, monthly_data], ignore_index=True) if not annual_data.empty else monthly_data
            if annual_data.empty:
                logger.warning(f"No data for the year {year}")
                return
            logger.info(f"Data for {year} ingested successfully")
            upload_parquet(minio_client, os.getenv("MINIO_BUCKET_NAME"), annual_data, f"{trade_type}_{data_type}/total_{trade_type}_{data_type}{year}.parquet")
    return None

def main():
    minio_client = get_minio_client(
        os.getenv("MINIO_EXTERNAL_URL"),
        os.getenv("MINIO_ACCESS_KEY"),
        os.getenv("MINIO_SECRET_KEY"),
        )
    # for data_type in ["hs", "statehs", "porths"]:
    #     ingest_all_data(minio_client, data_type)
    ingest_all_data(minio_client, "statehs")
    logger.info("Data uploaded to Minio")

if __name__ == "__main__":
    main()
