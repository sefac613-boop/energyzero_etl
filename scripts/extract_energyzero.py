import requests
import json
from datetime import datetime, timezone
import os

def extract_energyzero():
    url = "https://api.energyzero.nl/v1/energyprices"
    
    params = {
        "fromDate": "2025-01-01T00:00:00.000Z",
        "tillDate": "2025-01-02T00:00:00.000Z",
        "interval": 4,
        "usageType": 1,
        "inclBtw": "true"
    }
    
    response = requests.get(url, params=params)
    data = response.json()
    
    os.makedirs("/opt/airflow/data/raw", exist_ok=True)
    filename = f"/opt/airflow/data/raw/energyzero_{datetime.now(timezone.utc).strftime('%Y%m%d')}.json"
    
    with open(filename, "w") as f:
        json.dump(data, f, indent=2)
    
    print(f"✅ Veri kaydedildi: {filename}")
    return filename

if __name__ == "__main__":
    extract_energyzero()