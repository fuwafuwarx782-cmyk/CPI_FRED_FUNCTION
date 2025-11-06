import requests
import os
from datetime import datetime, timedelta

FRED_API_KEY = os.environ.get("FRED_API_KEY") 
SERIES_ALL = "JPNCPICONS"        # 総合指数
SERIES_FOOD = "JPNCPCFOOD"       # 食料指数
SERIES_ENERGY = "JPNCPCENRG"     # エネルギー指数
# ---------------------------

def get_cpi_from_fred():
    
    today = datetime.now()
    start_date = (today - timedelta(days=365)).strftime("%Y-%m-%d")
    
    all_series_map = {
        "all": SERIES_ALL, 
        "food": SERIES_FOOD, 
        "energy": SERIES_ENERGY
    }
    
    fetched_data = { #ここに辞書型でデータを入れる
        "indices": {}
    }
    
    fred_url = "https://api.stlouisfed.org/fred/series/observations"
    
    for key, series_id in all_series_map.items():
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