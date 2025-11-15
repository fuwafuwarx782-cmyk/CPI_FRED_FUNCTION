from S3_common_load import S3_common_load
from S3_common_save import S3_common_save
from format_for_csv import format_for_csv # 既存のCSV整形関数

import pandas as pd
import os
from datetime import datetime

ACTUAL_GDP_KEY = os.environ.get("ACTUAL_GDP_KEY") 
POTENTIAL_GDP_KEY = os.environ.get("POTENTIAL_GDP_KEY") 
GAP_PREFIX = "gdp_output_gap" 

def calculate_output_gap(actual_key: str, potential_key: str):

    actual_df = S3_common_load(file_key=actual_key)
    potential_df = S3_common_load(file_key=potential_key)
    
    actual_df = actual_df.set_index('obtain_date')[['index_value']]
    actual_df.columns = ['actual_gdp']
    
    potential_df = potential_df.set_index('obtain_date')[['index_value']]
    potential_df.columns = ['potential_gdp']

    merged_df = actual_df.join(potential_df, how='inner')
    
    merged_df['output_gap_rate'] = (
        (merged_df['actual_gdp'] - merged_df['potential_gdp']) / merged_df['potential_gdp']
    ) * 100
    
    gap_series_dict = merged_df['output_gap_rate'].round(2).to_dict()
    
    obtain_date = datetime.now().strftime("%Y-%m-%d")
    file_name = f"OutputGap_rate_{obtain_date}.csv"
    
    formatted_csv_data = format_for_csv(
        index_series=gap_series_dict, 
        series_name="OUTPUT_GAP"
    )

    original_prefix = os.environ.get("S3_PREFIX") # 現在の環境変数を保存
    os.environ["S3_PREFIX"] = GAP_PREFIX # 新しいプレフィックスを設定
    
    S3_common_save(
        csv_string=formatted_csv_data,
        file_name=file_name
    )
    
    if original_prefix is not None:
        os.environ["S3_PREFIX"] = original_prefix
    else:
        del os.environ["S3_PREFIX"]
        
    return file_name