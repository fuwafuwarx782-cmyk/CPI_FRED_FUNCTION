import boto3
import os
from datetime import datetime

S3_BUCKET_NAME = os.environ.get("S3_BUCKET_NAME")
S3_PREFIX = "real_gdp_data" #プレフィックスはデータを取得する辞書のキーかつフォルダみたいなもの

def S3_common_save(csv_string: str, file_name: str):

    if not S3_BUCKET_NAME:
        raise ValueError("S3_BUCKET_NAMEが環境変数に設定されていません！")
        
    try:
        s3 = boto3.client('s3') #接続開始
        s3_key = f"{S3_PREFIX}/{file_name}" #ファイルのパスを指定
        
        s3.put_object(
            Bucket=S3_BUCKET_NAME,
            Key=s3_key,
            Body=csv_string.encode('utf-8')
        )
        print(f"データをS3に保存完了！ Key: {s3_key}")
        
    except Exception as e:
        print(f"S3への保存中にエラー発生！: {e}")
        raise