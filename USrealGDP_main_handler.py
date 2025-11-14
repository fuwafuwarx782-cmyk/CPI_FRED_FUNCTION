import os
from datetime import datetime
import json

from S3_common_save import S3_common_save as save_to_s3 
from RealGDP_YOY import RealGDP_YOY
from fred_US_50yearsGDP_obtain import get_data_from_fred
from format_for_csv import format_for_csv

def lambda_handler(event, context):

    SERIES_KEY = "US_REAL_GDP_YOY" #CSVファイル名の一部

    obtain_date = datetime.now().strftime("%Y-%m-%d") #API取得日

    file_name = f"{SERIES_KEY}_rate_{obtain_date}.csv"

    real_gdp_config = {"series_id": "GDPC1", "key": "real_gdp"}
    
    try:
        #FREDから実質GDP指数値を取得
        fetched_data = get_data_from_fred(series_config=real_gdp_config)

        index_series = fetched_data["indices"]["real_gdp"] 
        
        if not index_series:
            raise ValueError("FREDから取得したデータが空です！")

        #YOYに計算
        yoy_rates_list = RealGDP_YOY(index_series=index_series)
        
        #CSV形式に整形
        yoy_rate_dict = {item["obtain_date"]: item["yoy_rate"] for item in yoy_rates_list if item["yoy_rate"] is not None}

        formatted_csv_data = format_for_csv(
            index_series=yoy_rate_dict, 
            series_name=SERIES_KEY
        )

        #S3に保存
        save_to_s3(
            csv_string=formatted_csv_data,
            file_name=file_name
        )
        
        return {"statusCode": 200, "body": json.dumps({"message": f"実質GDP成長率データをS3に保存完了: {file_name}"})}
        
    except Exception as e:
        print(f"エラーが発生！: {e}")
        raise e