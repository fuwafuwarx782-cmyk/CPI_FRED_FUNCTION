from typing import Dict, Any, List
from datetime import datetime, timedelta
import collections

def RealGDP_YOY(index_series: Dict[str, float]):
    
    #index_seriesはハンドラ関数でfetched_dataの日付と値を代入する変数
    yoy_rates_list = []

    sorted_dates = sorted(index_series.keys())
    for current_date_str in sorted_dates:
        current_value = index_series[current_date_str]

        current_date = datetime.strptime(current_date_str, "%Y-%m-%d")

        yoy_rate = None
        
        try:
            last_year_date_obj = current_date.replace(year=current_date.year - 1)
            last_year_date_str = last_year_date_obj.strftime("%Y-%m-%d")
        except ValueError:
            last_year_date_str = (current_date - timedelta(days=365)).strftime("%Y-%m-%d")

        last_year_value = index_series.get(last_year_date_str)

        
        if last_year_value is not None and last_year_value != 0:
            yoy_rate = (current_value / last_year_value - 1) * 100
        
        yoy_rates_list.append({
            "obtain_date": current_date_str, 
            "index_value": current_value,      
            "yoy_rate": round(yoy_rate, 2) if yoy_rate is not None else None,
        })
        
    return yoy_rates_list