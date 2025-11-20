import pandas as pd
import statsmodels.api as sm
import os
from datetime import datetime
from S3_common_load import S3_common_load 
from S3_common_save import S3_common_save

INPUT_FILE_KEY = os.environ.get('INPUT_FILE_KEY')

def model_execution_handler(event, context):
    
    df_merged = S3_common_load(INPUT_FILE_KEY) 
    
    Y = df_merged['GDP_YOY'].astype(float)
    X = df_merged['OUTPUT_GAP'].astype(float)
    X = sm.add_constant(X)

    model = sm.OLS(Y, X).fit()

    regression_equation = (
        f"GDP_YOY = {model.params['const']:.4f} + {model.params['OUTPUT_GAP']:.4f} * OUTPUT_GAP"
    )
    summary_content = (
        f"回帰式 : {regression_equation}\n"
        f"決定係数 : {model.rsquared:.4f}"
    )

    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    file_name = f"REGRESSION_SUMMARY_{timestamp}.txt"

    S3_common_save(csv_string=summary_content, file_name=file_name)