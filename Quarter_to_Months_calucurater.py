import pandas as pd
from typing import Literal

def resample_to_quarterly(df: pd.DataFrame, agg_method: Literal['mean', 'last'] = 'mean') -> pd.DataFrame:
    df['obtain_date'] = pd.to_datetime(df['obtain_date'])
    df = df.set_index('obtain_date')
    
    value_col = 'index_value' 
    
    if agg_method == 'mean':
        resampled_series = df[value_col].resample('QS').mean()
    elif agg_method == 'last':
        resampled_series = df[value_col].resample('QS').last()
    else:
        raise ValueError(f"Unknown aggregation method: {agg_method}")
    
    resampled_df = resampled_series.reset_index()
    resampled_df = resampled_df.rename(columns={resampled_df.columns[1]: value_col})
    resampled_df['obtain_date'] = resampled_df['obtain_date'].dt.strftime('%Y-%m-%d')
    resampled_df['series_key'] = df['series_key'].iloc[0]
    
    return resampled_df[['series_key', 'obtain_date', value_col]]