class BronzeLayer:
    def __init__(self):
        pass

    def ingest_data_api(self, url):
        import pandas as pd
        import requests
        from io import StringIO

        # 1) Fetch the data
        response = requests.get(url)
        if response.status_code == 200:
            data = response.text
            # Pandas cannot read text to it needs StringIO
            df = pd.read_csv(StringIO(data))

            # Converting text data to csv within memory using StringIO
            csv_buffer = StringIO()
            df.to_csv(csv_buffer, index=False)
            # Printing CSV content to check if it worked
            return csv_buffer.getvalue().encode("utf-8")

        else:
            print(f"Failed to fetch the data. Status Code: {response.status_code}")

    def put_data_s3(self, bucket_name, object_key, data):
        import os
        from dotenv import load_dotenv
        import boto3

        load_dotenv()

        aws_access_key_id = os.environ.get("aws_access_key_id")
        aws_secret_access_key = os.environ.get("aws_secret_access_key")
        # 2) Creating client to connect to the bucket
        s3_client = boto3.client(
            's3',
            region_name = 'ca-central-1',
            aws_access_key_id = aws_access_key_id,
            aws_secret_access_key = aws_secret_access_key
        )
        # 3) Loading the data to S3 bucket
        s3_client.put_object(
            Bucket=bucket_name,
            Key=object_key,
            Body=data
        )

        return f"Data uploaded to S3 bucket {bucket_name} with object key '{object_key}'"

