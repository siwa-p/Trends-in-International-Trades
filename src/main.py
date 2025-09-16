import asyncio
import os
import sys
from pathlib import Path

from dotenv import load_dotenv
from fastapi import FastAPI

parent_dir = Path(__file__).resolve().parent.parent
sys.path.append(str(parent_dir))
from datetime import datetime

from src.utils.logger_config import logger
from src.utils.utilities import get_dremio_connection, query_table

load_dotenv(override=True)

dremio_port = int(os.getenv("DREMIO_PORT", "32010"))
dremio_host = os.getenv("DREMIO_HOST")
dremio_password = os.getenv("DREMIO_PASSWORD")
dremio_user = os.getenv("DREMIO_USER")
dremio_conn = get_dremio_connection(
    dremio_user, dremio_password, dremio_host, dremio_port
)
app = FastAPI()


@app.get("/healthcheck", tags=["Health Check"])
async def healthcheck():
    return {"status": "ok", "datetime": datetime.utcnow().isoformat()}


@app.get("/wits_tariff_trade", tags=["WITS Tariff and Trade Data"])
async def read_wits_trade_tariff(
    data_type: str = "trade",
    offset: int = 0,
    limit: int = 10,
    partner: str = None,
    year: int = None,
):
    try:
        filters = []
        if partner:
            filters.append(f"PARTNER_CTY = '{partner}'")
        if year:
            filters.append(f"TIME_PERIOD = {year}")

        where_clause = ""
        if filters:
            where_clause = "WHERE " + " AND ".join(filters)

        query = f"""
            SELECT *
            FROM nessie.staged.staged_wits_{data_type}
            {where_clause}
            LIMIT {limit} OFFSET {offset}
        """
        tariff_pd = query_table(dremio_conn, query)
        return tariff_pd.to_dict(orient="records")
    except Exception as e:
        logger.error(f"Error reading WITS {data_type} data: {e}")
        return {"error": str(e)}


@app.get("/import_export_hs", tags=["Monthly Import Export by Commodity"])
async def read_hs_data(
    data_type: str = "import",
    country_name: str = None,
    year: int = None,
    month: int = None,
    limit: int = 10,
    offset: int = 0,
):
    try:
        filters = []
        if country_name:
            filters.append(f"CTY_NAME = '{country_name}'")
        if year:
            if data_type == "export":
                filters.append(f"EXPORT_YEAR = {year}")
            else:
                filters.append(f"IMPORT_YEAR = {year}")
        if month:
            if data_type == "export":
                filters.append(f"EXPORT_MONTH = {month}")
            else:
                filters.append(f"IMPORT_MONTH = {month}")

        where_clause = ""
        if filters:
            where_clause = "WHERE " + " AND ".join(filters)

        if data_type == "export":
            commodity_col = "E_COMMODITY"
            desc_col = "E_COMMODITY_SDESC"
            value_col = "ALL_VAL_MO"
        else:
            commodity_col = "I_COMMODITY"
            desc_col = "I_COMMODITY_SDESC"
            value_col = "GEN_VAL_MO"

        group_by_clause = f"GROUP BY {commodity_col}, {desc_col}"
        query = f"""
            SELECT {commodity_col}, {desc_col}, SUM({value_col}) as total_value
            FROM nessie.staged.staged_{data_type}_hs
            {where_clause}
            {group_by_clause}
            LIMIT {limit} OFFSET {offset}
        """
        tariff_pd = query_table(dremio_conn, query)
        tariff_pd = tariff_pd.fillna("-")
        return tariff_pd.to_dict(orient="records")
    except Exception as e:
        logger.error(f"Error reading {data_type} hs data: {e}")
        return {"error": str(e)}


@app.get("/port_import_export", tags=["Port Level Import Export Data"])
async def read_port_data(
    data_type: str = "import",
    country_name: str = None,
    year: int = None,
    limit: int = 10,
    offset: int = 0,
):
    try:
        filters = []
        if country_name:
            filters.append(f"CTY_NAME = '{country_name}'")
        if year:
            if data_type == "export":
                filters.append(f"EXPORT_YEAR = {year}")
            else:
                filters.append(f"IMPORT_YEAR = {year}")
        where_clause = ""
        if filters:
            where_clause = "WHERE " + " AND ".join(filters)
        query = f"""SELECT * FROM nessie.staged.staged_{data_type}_port 
                    {where_clause}
                    LIMIT {limit} OFFSET {offset}
                """
        tariff_pd = query_table(dremio_conn, query)
        tariff_pd = tariff_pd.fillna("-")
        return tariff_pd.to_dict(orient="records")
    except Exception as e:
        logger.error(f"Error reading bronze port level {data_type} data: {e}")
        return {"error": str(e)}


@app.get("/state_import_export", tags=["State Level Import Export Data"])
async def read_state_data(
    data_type: str = "import",
    country_name: str = None,
    year: int = None,
    state: str = None,
    limit: int = 10,
    offset: int = 0,
):
    try:
        filters = []
        if country_name:
            filters.append(f"CTY_NAME = '{country_name}'")
        if state:
            filters.append(f"STATE_ID = '{state}'")
        if year:
            if data_type == "export":
                filters.append(f"EXPORT_YEAR = {year}")
            else:
                filters.append(f"IMPORT_YEAR = {year}")
        where_clause = ""
        if filters:
            where_clause = "WHERE " + " AND ".join(filters)
        query = f"""SELECT * FROM nessie.staged.staged_{data_type}_state 
                    {where_clause}
                    LIMIT {limit} OFFSET {offset}
                """
        tariff_pd = query_table(dremio_conn, query)
        tariff_pd = tariff_pd.fillna("-")
        return tariff_pd.to_dict(orient="records")
    except Exception as e:
        logger.error(f"Error reading state level {data_type} data: {e}")
        return {"error": str(e)}


if __name__ == "__main__":
    import time

    start = time.time()
    asyncio.run(read_wits_trade_tariff())
    # asyncio.run(read_port_data())
    end = time.time()
    print(f"Time taken: {end - start} seconds")
