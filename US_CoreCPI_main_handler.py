import os
import sys
from datetime import datetime

from fred_US_50yearsCPI_obtain import get_us_core_cpi_from_fred 
from CoreCPI_YOY_ import CoreCPI_YOY       
from format_for_csv import format_for_csv  
from S3_common_save import S3_common_save

def lambda_handler(event, context):

    try:
        print("FREDからアメリカのコアCPI指数を取得します！")
        fetched_data = get_us_core_cpi_from_fred() 
        
        for series_key, index_series in fetched_data["indices"].items():
            
            print("YOY変化率を計算します！")
            yoy_rate_series = CoreCPI_YOY(index_series=index_series)

            timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
            file_name = f"{series_key}_YOY_rate_{timestamp}.csv"
            
            print("データをCSV形式に整形します！")
            csv_string = format_for_csv(
                index_series=yoy_rate_series, 
                series_name=f"{series_key}_yoy_rate" 
            )
            
            print("データをS3に保存します！")
            S3_common_save(
                csv_string=csv_string, 
                file_name=file_name
            )

        print("全ての処理が正常に完了しました！")
        return {
            'statusCode': 200,
            'body': '処理に全部成功したよ！'
        }
        
    except Exception as e:
        print(f"エラーが発生しました！: {e}")
        return {
            'statusCode': 500,
            'body': f'エラーだよ！ {e}'
        }

#indicesがキーの辞書型の値にキーcorecpiで値に辞書が入っている
# 辞書があって、これがseries_keyとindex_seriesに入っていて、
# 値index_seriesは辞書でキー時間と値に指数が入っているから、
# これをYOYに渡していると…