import json
import urllib.parse
import boto3
import re
import os
from datetime import datetime

s3 = boto3.client('s3')

pattern = os.environ.get('FILENAME_FORMAT')
target_bucket_correct = 'genkoukanri-test2'
target_bucket_incorrect = 'genkoukanri-test3'

def lambda_handler(event, context):

    if not pattern:
        print("フォーマットの環境変数が設定されていません！")
        return {
            'statusCode': 500,
            'body': json.dumps('設定エラー: ファイル名パターンが未定義です。')
        }
        
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    
    match = re.match(pattern, key)
    
    if match:
        year = match.group(1)
        month = match.group(2)
        day = match.group(3)
        book_name = match.group(4)

        new_key = f'{year}/{month}/{day}/{book_name}.docx'
        
        try:
            copy_source = {'Bucket': bucket_name, 'Key': key}
            
            s3.copy_object(
                CopySource=copy_source,
                Bucket=target_bucket_correct,
                Key=new_key
            )
            
            s3.delete_object(
                Bucket=bucket_name,
                Key=key
            )
            
            print(f"ファイル {key} を {target_bucket_correct}/{new_key} へ移動しました！")
            
            return {
                'statusCode': 200,
                'body': json.dumps(f'ファイルは {target_bucket_correct}/{new_key} へ保存されました！')
            }
            
        except Exception as e:
            print(e)
            print(f"ファイル {key} の処理中にエラーが発生しました！")
            raise e
            
    else:
        unmatched_key = key
        
        try:
            copy_source = {'Bucket': bucket_name, 'Key': key}
            
            s3.copy_object(
                CopySource=copy_source,
                Bucket=target_bucket_incorrect,
                Key=unmatched_key
            )
            
            s3.delete_object(
                Bucket=bucket_name,
                Key=key
            )
            
            print(f"ファイル {key} はフォーマット不一致のため {target_bucket_incorrect}/{unmatched_key} へ移動されました！")
            return {
                'statusCode': 200,
                'body': json.dumps(f'ファイル名がフォーマットに一致しなかったため、{target_bucket_incorrect} へ移動しました！')
            }

        except Exception as e:
            print(e)
            print(f"ファイル {key} の移動中にエラーが発生しました！")
            raise e