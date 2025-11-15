from typing import Dict, Any, List
from datetime import datetime, timedelta

def CoreCPI_YOY(index_series: Dict[str, float]):
    
    sorted_dates = sorted(index_series.keys())
    
    yoy_rates_dict = {} #ここに保存する
    
    for current_date_str in sorted_dates:
        current_index = index_series[current_date_str]
        current_date = datetime.strptime(current_date_str, "%Y-%m-%d")

        yoy_rate = None
        
        last_year_date = (current_date - timedelta(days=365)).strftime("%Y-%m-%d")

        last_year_index = index_series.get(last_year_date)
        
        if last_year_index is not None and last_year_index != 0:
            # YoY = (当月指数 / 前年同月指数 - 1) * 100
            yoy_rate = (current_index / last_year_index - 1) * 100
        
        if yoy_rate is not None:
            yoy_rates_dict[current_date_str] = round(yoy_rate, 2)
            
    return yoy_rates_dict # 辞書を返す