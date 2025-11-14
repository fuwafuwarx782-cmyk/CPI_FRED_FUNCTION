import os
from datetime import datetime
import json

from fred_US_50yearsCPI_obtain import get_us_core_cpi_from_fred
from fred_APIdata_format_for_csv import format_for_s3_csv
from S3_common_save import save_to_s3

def lambda_handler(event, context):

    SERIES_KEY = "US_CORE_CPI_50Y" 

    execution_date = datetime.now().strftime("%Y-%m-%d")
    file_name = f"{SERIES_KEY}_index_{execution_date}.csv"

    try:
        fetched_data = get_us_core_cpi_from_fred()

        index_series = fetched_data["indices"]["core_cpi"] 
        
        if not index_series:
            raise ValueError("取得したデータが空です！")

        formatted_csv_data = format_for_s3_csv(
            index_series=index_series, 
            series_name=SERIES_KEY
        )

        save_to_s3(
            csv_string=formatted_csv_data,
            file_name=file_name
        )
        
        return {"statusCode": 200, "body": "Success"}
        
    except Exception as e:
        print(f"エラーが発生！: {e}")

        raise e