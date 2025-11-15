import requests
import os
from datetime import datetime, timedelta
from typing import Dict, Any

FRED_API_KEY = os.environ.get("FRED_API_KEY") 

SERIES_CORE = {
    "official_gdp_gap": "GDPPOT" 
}

def get_data_from_fred(series_config: dict = None) -> Dict[str, Any]:
    
    today = datetime.now()
    start_date = (today - timedelta(days=18500)).strftime("%Y-%m-%d")
    
    fetched_data = {
        "indices": {}
    }
    
    fred_url = "https://api.stlouisfed.org/fred/series/observations"
    
    for key, series_id in SERIES_CORE.items():
        params = {
            "series_id": series_id,
            "api_key": FRED_API_KEY,
            "file_type": "json",
            "observation_start": start_date,
            "frequency": "q" 
        }
        
        try:
            print(f"FREDとの通信を開始するよ！シリーズID: {series_id}")
            response = requests.get(fred_url, params=params)
            response.raise_for_status() 
            data = response.json()
            
            fetched_data["indices"][key] = {}
            for obs in data.get('observations', []):
                date = obs['date']
                value = obs['value']
                
                if value != '.': 
                    fetched_data["indices"][key][date] = float(value)
            print(f"FREDからのデータ取得に成功したよ！シリーズ: {key}")
                    
        except requests.exceptions.RequestException as e:
            print(f"APIの取得に失敗しました！ {series_id}: {e}")
            raise ConnectionError(f"FREDからのデータ取得に失敗したよ！: {e}")

    return fetched_data