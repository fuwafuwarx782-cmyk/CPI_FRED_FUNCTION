#データが辞書型でindex_seriesとseries_nameがあれば使える
from typing import Dict, Any, List
import csv
from io import StringIO 
from datetime import datetime

def format_for_csv(index_series: Dict[str, float], series_name: str):

    fieldnames = [
        'series_key',  # STRING: データの種類を識別するためのキー
        'obtain_date', # STRING: データ取得日 (YYYY-MM-DD形式)
        'index_value'  # DOUBLE: 指数値
    ]

    output = StringIO()
    writer = csv.DictWriter(output, fieldnames=fieldnames, extrasaction='ignore')
    writer.writeheader() 
    
    sorted_dates = sorted(index_series.keys())

    for date_str in sorted_dates:
        index_val = index_series[date_str]

        row = {
            'series_key': series_name.upper(), 
            'obtain_date': date_str,
            'index_value': index_val,
        }
        
        writer.writerow(row)
        
    return output.getvalue()