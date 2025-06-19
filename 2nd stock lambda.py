import json
import boto3
import os
import psycopg2
import pandas as pd


def get_latest_s3_object_key(bucket_name):
    s3 = boto3.client('s3')
    response = s3.list_objects_v2(Bucket=bucket_name)

    if 'Contents' not in response:
        return None

    # Sort by LastModified timestamp, descending
    sorted_objects = sorted(response['Contents'], key=lambda obj: obj['LastModified'], reverse=True)
    return sorted_objects[0]['Key']


def lambda_handler(event, context):
    host = os.environ['DB_HOST']
    database = os.environ['DB_NAME']
    user = os.environ['DB_USER']
    password = os.environ['DB_PASS']
    port = os.environ['DB_PORT']

    bucket_name = 'stockdata08'
    s3 = boto3.client('s3')

    key_key = get_latest_s3_object_key(bucket_name)
    if not key_key:
        return {'statusCode': 404, 'body': json.dumps('No stock data files found in S3 bucket.')}

    print(f"Processing file: {key_key}")

    try:
        response = s3.get_object(Bucket=bucket_name, Key=key_key)
        data = json.loads(response['Body'].read())

        meta_data = data.get("Meta Data", {})
        time_series = data.get("Time Series (5min)", {})

        if not time_series:
            return {'statusCode': 200, 'body': json.dumps('No time series data found in latest file.')}

        # Convert time series to DataFrame
        df = pd.DataFrame.from_dict(time_series, orient='index')
        df.reset_index(inplace=True)
        df.rename(columns={
            'index': 'timestamp',
            '1. open': 'open',
            '2. high': 'high',
            '3. low': 'low',
            '4. close': 'close',
            '5. volume': 'volume'
        }, inplace=True)

        df['symbol'] = meta_data.get("2. Symbol")
        df['interval'] = meta_data.get("4. Interval")
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df.dropna(inplace=True)

        print(f"Total rows parsed: {len(df)}")

        # Connect to PostgreSQL
        conn = psycopg2.connect(
            host=host,
            database=database,
            user=user,
            password=password,
            port=port
        )
        cursor = conn.cursor()

        # Create table if not exists
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS stock_intraday (
                symbol TEXT NOT NULL,
                timestamp TIMESTAMPTZ PRIMARY KEY,
                open NUMERIC,
                high NUMERIC,
                low NUMERIC,
                close NUMERIC,
                volume INTEGER,
                interval TEXT
            );
        """)
        conn.commit()
        print("Table check/creation completed.")

        # OPTIONAL: Clear table before insert (only if needed)
        # cursor.execute("TRUNCATE TABLE stock_intraday;")
        # conn.commit()
        # print("Table truncated.")

        # Insert rows with conflict handling
        inserted_count = 0
        skipped_count = 0

        for _, row in df.iterrows():
            try:
                cursor.execute("""
                    INSERT INTO stock_intraday (
                        symbol, timestamp, open, high, low, close, volume, interval
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (timestamp) DO NOTHING;
                """, (
                    row['symbol'], row['timestamp'], row['open'], row['high'],
                    row['low'], row['close'], int(row['volume']), row['interval']
                ))
                if cursor.rowcount == 1:
                    inserted_count += 1
                else:
                    skipped_count += 1
            except Exception as e:
                print(f"Error inserting row for {row['timestamp']}: {e}")
                continue

        conn.commit()
        cursor.close()
        conn.close()

        print(f"Insertion completed. Inserted: {inserted_count}, Skipped: {skipped_count}")

        return {
            'statusCode': 200,
            'body': json.dumps(f'Stock data from {key_key} processed. Inserted: {inserted_count}, Skipped: {skipped_count}')
        }

    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps(f"Processing error: {str(e)}")
        }