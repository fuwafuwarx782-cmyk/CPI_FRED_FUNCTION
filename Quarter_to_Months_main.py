import os
from datetime import datetime
from S3_common_load import S3_common_load
from S3_common_save_QtoM import S3_common_save
from Quarter_to_Months_calucurater import resample_to_quarterly
import pandas as pd

INPUT_S3_KEY = os.environ.get("INPUT_S3_KEY")
OUTPUT_S3_PREFIX = os.environ.get("OUTPUT_S3_PREFIX")
SERIES_KEY = os.environ.get("SERIES_KEY", "RESAMPLED_DATA")
AGGREGATION_METHOD = os.environ.get("AGGREGATION_METHOD", "mean") 

def lambda_handler(event, context):

    try:
        monthly_df = S3_common_load(file_key=INPUT_S3_KEY)

        quarterly_df = resample_to_quarterly(
            df=monthly_df, 
            agg_method=AGGREGATION_METHOD
        )
        
        csv_string = quarterly_df.to_csv(index=False)
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        file_name = f"{SERIES_KEY}_quarterly_{timestamp}.csv"
        
        S3_common_save(
            csv_string=csv_string, 
            file_name=file_name,
            s3_prefix=OUTPUT_S3_PREFIX
        ) 

        return {
            'statusCode': 200,
            'body': '処理に成功したよ！'
        }

    except Exception as e:
        return {
            'statusCode': 500,
            'body': '処理に失敗したよ！{e}'
        }