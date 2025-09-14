import os
import sys
from fastapi.testclient import TestClient
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from src.main import app

client = TestClient(app)


def test_trade_tariff_endpoint():
    response = client.get("/wits_tariff_trade?limit=2")
    assert response.status_code == 200
    assert isinstance(response.json(), list)
    assert len(response.json()) <= 2
    data = response.json()
    if data:
        record = data[0]
        assert "posting_id" in record
        assert isinstance(record["posting_id"], int)
    