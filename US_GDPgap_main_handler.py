import os
import sys
from datetime import datetime

from fred_US_GDPgap_obtain import get_data_from_fred, SERIES_CORE
from format_for_csv import format_for_csv
from S3_common_save import S3_common_save

def lambda_handler(event, context):

    try:
        print("FREDからデータを取得します！")
        fetched_data = get_data_from_fred(series_config=SERIES_CORE)
        
        for series_key, index_series in fetched_data["indices"].items():
            
            timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
            file_name = f"{series_key}_{timestamp}.csv"
            
            print("データをCSV形式に整形します！")
            csv_string = format_for_csv(
                index_series=index_series, 
                series_name=series_key
            )
            
            print("データをS3に保存します！")
            S3_common_save(
                csv_string=csv_string, 
                file_name=file_name
            )

        print("全ての処理が正常に完了しました！")

        return {
            'statusCode': 200,
            'body': 'FRED Data acquisition and S3 upload completed successfully.'
        }
        
    except Exception as e:
        # ログ出力と、Lambdaにエラーを通知
        print(f"エラーが発生しました！: {e}")
        return {
            'statusCode': 500,
            'body': f'An error occurred: {e}'
        }