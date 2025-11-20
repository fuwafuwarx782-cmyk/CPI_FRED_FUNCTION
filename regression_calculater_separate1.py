import pandas as pd
import os
from S3_common_load import S3_common_load 
from S3_common_save import S3_common_save

Y_S3_KEY = os.environ.get('Y_S3_KEY')
X_S3_KEY = os.environ.get('X_S3_KEY')
OUTPUT_FILE_KEY = 'merged_regression_data.csv'

def data_preparation_handler(event, context):

    df_gdp = S3_common_load(Y_S3_KEY).rename(columns={'index_value': 'GDP_YOY'})
    df_gap = S3_common_load(X_S3_KEY).rename(columns={'index_value': 'OUTPUT_GAP'})

    df_merged = pd.merge(
        df_gdp[['obtain_date', 'GDP_YOY']],
        df_gap[['obtain_date', 'OUTPUT_GAP']],
        on='obtain_date',
        how='inner'
    ).dropna()
    
    csv_string = df_merged.to_csv(index=False)
    S3_common_save(csv_string=csv_string, file_name=OUTPUT_FILE_KEY)