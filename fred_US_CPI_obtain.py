#アメリカのコアCPIを取得するためだけの関数。
import requests
import os
from datetime import datetime, timedelta

FRED_API_KEY = os.environ.get("FRED_API_KEY") 

def get_us_core_cpi_from_fred():
    
    today = datetime.now()
    start_date = (today - timedelta(days=420)).strftime("%Y-%m-%d")
    
    SERIES_CORE = {
        "core_cpi": "CPILFENS"
    }
    
    fetched_data = { #ここに辞書型でデータを入れる
        "indices": {}
    }
    
    fred_url = "https://api.stlouisfed.org/fred/series/observations"
    
    for key, series_id in SERIES_CORE.items():
        params = {
            "series_id": series_id,
            "api_key": FRED_API_KEY,
            "file_type": "json",
            "observation_start": start_date,
            "frequency": "m" 
        }
        
        try:
            print("FREDとの通信を開始するよ！")
            response = requests.get(fred_url, params=params)
            response.raise_for_status() 
            data = response.json()
            
            fetched_data["indices"][key] = {}
            for obs in data.get('observations', []):
                date = obs['date']
                value = obs['value']
                
                if value != '.': 
                    fetched_data["indices"][key][date] = float(value)
            print("FREDからのデータ取得に成功したよ！")
                    
        except requests.exceptions.RequestException as e:
            print(f"APIの取得に失敗しました！ {series_id}: {e}")
            raise ConnectionError(f"FREDからのデータ取得に失敗したよ！: {e}")

    return fetched_data