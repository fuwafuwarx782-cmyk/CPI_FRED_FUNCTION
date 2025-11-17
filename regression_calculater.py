import pandas as pd
import statsmodels.api as sm
import numpy as np
from datetime import datetime
from S3_common_load import S3_common_load 
from S3_common_save import S3_common_save

Y_S3_KEY = 'US_REAL_GDP_YOY_rate_2025-11-15.csv'
X_S3_KEY = 'OutputGap_rate_2025-11-17 (2).csv'

def run_s3_regression():
    print("S3からデータをロードするよ")
    
    try:
        df_gdp = S3_common_load(Y_S3_KEY).rename(columns={'index_value': 'GDP_YOY'})
        df_gap = S3_common_load(X_S3_KEY).rename(columns={'index_value': 'OUTPUT_GAP'})
    except Exception as e:
        print("S3からのデータロード中にエラーが発生しました！{e}")
        return

    df_merged = pd.merge(
        df_gdp[['obtain_date', 'GDP_YOY']],
        df_gap[['obtain_date', 'OUTPUT_GAP']],
        on='obtain_date',
        how='inner'
    ).dropna()

    Y = df_merged['GDP_YOY'].astype(float)
    X = df_merged['OUTPUT_GAP'].astype(float)
    X = sm.add_constant(X)

    model = sm.OLS(Y, X).fit()

    print("\n--- S3データによる回帰分析結果 ---")
    print(f"回帰式: GDP_YOY = {model.params['const']:.4f} + {model.params['OUTPUT_GAP']:.4f} * OUTPUT_GAP")
    print(f"OUTPUT_GAPのP値: {model.pvalues['OUTPUT_GAP']:.4f}")
    print(f"決定係数 (R-squared): {model.rsquared:.4f}")
    print("----------------------------------")

    summary_content = (
        "\n--- S3データによる回帰分析結果 ---\n"
        f"回帰式: GDP_YOY = {model.params['const']:.4f} + {model.params['OUTPUT_GAP']:.4f} * OUTPUT_GAP\n"
        f"OUTPUT_GAPのP値: {model.pvalues['OUTPUT_GAP']:.4f}\n"
        f"決定係数 (R-squared): {model.rsquared:.4f}\n"
        "----------------------------------\n"
    )

    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    file_name = f"REGRESSION_SUMMARY_{timestamp}.txt"

    S3_common_save(csv_string=summary_content, file_name=file_name)
    
    print(f" 回帰分析サマリーをS3に保存完了。ファイル名: {file_name}")

if __name__ == '__main__':
    run_s3_regression()