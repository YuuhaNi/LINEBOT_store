import json
import boto3
import logging
import os
import urllib.request
import time
from linebot import LineBotApi
from datetime import datetime

logger = logging.getLogger()
logger.setLevel(logging.INFO)

from boto3.dynamodb.conditions import Key

dynamodb = boto3.resource('dynamodb')
table_name = os.environ['TABLE_NAME']  # 環境変数からテーブル名を取得
table = dynamodb.Table(table_name)

line_bot_api = LineBotApi(os.environ["CHANNEL_ACCESS_TOKEN"])  # 環境変数からCHANNEL_ACCESS_TOKENを取得

s3 = boto3.client('s3')

# テーブルスキャン
def operation_scan():
    scanData = table.scan()
    items = scanData['Items']
    print(items)
    return scanData

# レコード検索
def operation_query(user_id, timestamp):
    queryData = table.query(
        KeyConditionExpression=Key("userId").eq(user_id) & Key("timestamp").eq(timestamp)
    )
    items = queryData['Items']
    print(items)
    return items

# レコード追加・更新
def operation_put(user_id, timestamp, display_name, message_text, image_url=None):
    putResponse = table.update_item(
        Key={
            'userId': user_id,
            'timestamp': timestamp
        },
        UpdateExpression='SET #dn = :dn, #mt = :mt, #iu = :iu',
        ExpressionAttributeNames={
            '#dn': 'display_name',
            '#mt': 'message_text',
            '#iu': 'image_url'
        },
        ExpressionAttributeValues={
            ':dn': display_name,
            ':mt': message_text,
            ':iu': image_url
        },
        ReturnValues='UPDATED_NEW'
    )
    if putResponse['ResponseMetadata']['HTTPStatusCode'] != 200:
        print(putResponse)
    else:
        print('PUT Successed.')
    return putResponse

# レコード削除
def operation_delete(user_id, timestamp):
    delResponse = table.delete_item(
        Key={
            'userId': user_id,
            'timestamp': timestamp
        }
    )
    if delResponse['ResponseMetadata']['HTTPStatusCode'] != 200:
        print(delResponse)
    else:
        print('DEL Successed.')
    return delResponse

def lambda_handler(event, context):
    logger.info("Received event: " + json.dumps(event))
    print("Received event: " + json.dumps(event))
    
    # LINEのリクエストからeventを取得する
    for message_event in json.loads(event["body"])["events"]:
        logger.info(json.dumps(message_event))
        
        if "replyToken" in message_event:
            user_id = message_event["source"]["userId"]
            timestamp = int(time.time())  # 現在のUNIXタイムスタンプを整数で取得
            
            # ユーザーの表示名を取得
            profile = line_bot_api.get_profile(user_id)
            display_name = profile.display_name
            
            if message_event["message"]["type"] == "image":
                # イメージメッセージの処理
                message_content = line_bot_api.get_message_content(message_event["message"]["id"])
                image_data = message_content.content
                
                date = datetime.fromtimestamp(timestamp).strftime('%y-%m-%d-%H-%M-%S')  # 日時の形式を変更
                file_name = f"{display_name}/{date}.jpg"  # ファイル名の形式を表示名/日時.jpgに変更
                bucket_name = os.environ['BUCKET_NAME']  # 環境変数からバケット名を取得
                
                s3.put_object(Bucket=bucket_name, Key=file_name, Body=image_data)
                
                image_url = f"https://{bucket_name}.s3.amazonaws.com/{file_name}"
                operation_put(user_id, timestamp, display_name, None, image_url)
                
                reply_text = "画像が保存されたよ！"
            else:
                # テキストメッセージの処理
                message_text = message_event["message"]["text"]
                operation_put(user_id, timestamp, display_name, message_text)
                
                reply_text = f"表示名: {display_name}, メッセージ: {message_text}"
            
            url = "https://api.line.me/v2/bot/message/reply"
            headers = {
                "Content-Type": "application/json",
                "Authorization": "Bearer " + os.environ["CHANNEL_ACCESS_TOKEN"]
            }
            
            data = {
                "replyToken": message_event["replyToken"],
                "messages": [
                    {
                        "type": "text",
                        "text": reply_text,
                    }
                ]
            }
            
            req = urllib.request.Request(url=url, data=json.dumps(data).encode("utf-8"), method="POST", headers=headers)
            
            with urllib.request.urlopen(req) as res:
                logger.info(res.read().decode("utf-8"))
                return {
                    "statusCode": 200,
                    "body": json.dumps("Hello from Lambda!")
                }
    
    return {
        "statusCode": 200,
        "body": json.dumps("Hello from Lambda!")
    }