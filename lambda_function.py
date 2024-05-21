import json
import boto3
import logging
import os
import urllib.request
import time
from linebot import LineBotApi
from datetime import datetime, timedelta

logger = logging.getLogger()
logger.setLevel(logging.INFO)

from boto3.dynamodb.conditions import Key

dynamodb = boto3.resource('dynamodb')
table_name = os.environ['TABLE_NAME']  # 環境変数からテーブル名を取得
table = dynamodb.Table(table_name)

line_bot_api = LineBotApi(os.environ["CHANNEL_ACCESS_TOKEN"])

s3 = boto3.client('s3')
rekognition = boto3.client('rekognition')
model_arn = os.environ['REKOGNITION_MODEL_ARN']   # カスタムラベルモデルのARN

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
                
                # 日本時間を取得
                jp_time = datetime.utcfromtimestamp(timestamp) + timedelta(hours=9)
                date = jp_time.strftime('%y-%m-%d-%H-%M-%S')  # 日時の形式を変更
                file_name = f"{date}.jpg"  # ファイル名の形式を日時.jpgに変更
                bucket_name = os.environ['BUCKET_NAME']  # 環境変数からバケット名を取得
                
                # 画像をS3バケットに保存
                s3.put_object(Bucket=bucket_name, Key=file_name, Body=image_data)
                
                # 画像の分類
                response = rekognition.detect_custom_labels(
                    Image={'S3Object': {'Bucket': bucket_name, 'Name': file_name}},
                    ProjectVersionArn=model_arn
                )
                
                # 分類結果の取得
                labels = response['CustomLabels']
                if len(labels) > 0:
                    top_label = labels[0]['Name']
                    confidence = labels[0]['Confidence']
                    reply_text = f"画像の分類結果: {top_label} (信頼度: {confidence:.2f}%)"
                else:
                    reply_text = "画像の分類結果: その他"
                
                image_url = f"https://{bucket_name}.s3.amazonaws.com/{file_name}"
                operation_put(user_id, timestamp, display_name, None, image_url)
            else:
                # テキストメッセージの処理
                message_text = message_event["message"]["text"]
                operation_put(user_id, timestamp, display_name, message_text)
                reply_text = f"表示名: {display_name}, メッセージ: {message_text}"
            
            # 分類結果をLINEで返信
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