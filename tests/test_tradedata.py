import pandas as pd
import os
import sys
import pytest
from pathlib import Path
from dotenv import load_dotenv
parent_dir = Path(__file__).resolve().parent.parent
sys.path.append(str(parent_dir))
from src.utils.utilities import get_dremio_connection, query_table
load_dotenv(override=True)

dremio_port = int(os.getenv("DREMIO_PORT", "32010"))
dremio_host = os.getenv("DREMIO_HOST")
dremio_password = os.getenv("DREMIO_PASSWORD")
dremio_user = os.getenv("DREMIO_USER")
dremio_conn = get_dremio_connection(
    dremio_user, dremio_password, dremio_host, dremio_port
)

@pytest.mark.integration
def test_trade_data():
    query = f"""
            select * from nessie.staged.staged_wits_trade limit 10
            """
    data = query_table(dremio_conn, query)
    assert isinstance(data, pd.DataFrame)
    assert len(data)>0
    
    
if __name__ == '__main__':
    test_trade_data()