import requests
import boto3
import json
from datetime import datetime

# === CONFIGURATION ===
API_KEY = "7YCMKUJ30LJX020C"  # Replace with your API key
SYMBOLS = ['TSLA']  # Stock symbols to query
BUCKET = "stockdata08"  # Your S3 bucket

# Initialize S3 client
s3 = boto3.client('s3')

# === GET STOCK DATA ===
def get_stock_data(symbol):
    url = "https://www.alphavantage.co/query"
    params = {
        "function": "TIME_SERIES_INTRADAY",
        "symbol": symbol,
        "interval": "5min",
        "apikey": API_KEY
    }
    response = requests.get(url, params=params)
    return response.json()

# === SAVE TO S3 ===
def save_to_s3(symbol, data):
    now = datetime.utcnow().strftime("%Y-%m-%dT%H-%M")
    key = f"{symbol}/{now}.json"
    s3.put_object(Bucket=BUCKET, Key=key, Body=json.dumps(data))
    print(f"✅ Saved to s3://{BUCKET}/{key}")

# === MAIN LAMBDA FUNCTION ===
def lambda_handler(event=None, context=None):
    try:
        for symbol in SYMBOLS:
            raw_data = get_stock_data(symbol)
            save_to_s3(symbol, raw_data)
        return {
            "statusCode": 200,
            "body": f"Stock data for {SYMBOLS} saved to S3."
        }
    except Exception as e:
        print("❌ Error:", str(e))
        return {
            "statusCode": 500,
            "body": f"Error: {str(e)}"
        }