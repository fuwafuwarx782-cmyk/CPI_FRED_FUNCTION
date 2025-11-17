import os
import sys
from datetime import datetime

from fred_US_50yearsGDP_obtain import get_data_from_fred
from format_for_csv import format_for_csv
from S3_common_save import S3_common_save

def lambda_handler(event, context):
    
    SERIES_CORE = {
        "key": "US_REAL_GDP", 
        "series_id": "GDPC1"   
    }

    try:
        fetched_data = get_data_from_fred(series_config=SERIES_CORE)
        
        for series_key, index_series in fetched_data["indices"].items():
            
            timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
            file_name = f"{series_key}_{timestamp}.csv"
            
            csv_string = format_for_csv(
                index_series=index_series, 
                series_name=series_key
            )
            
            S3_common_save(
                csv_string=csv_string, 
                file_name=file_name
            )

        return {
            'statusCode': 200,
            'body': 'FRED Data acquisition and S3 upload completed successfully.'
        }
        
    except Exception as e:
        return {
            'statusCode': 500,
            'body': f'An error occurred: {e}'
        }