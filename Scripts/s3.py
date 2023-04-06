import boto3
import pandas as pd
import io
import json


class S3Client:
    def __init__(self):
        self.client = boto3.client('s3')
        self.resource = boto3.resource('s3')

    def putDictionary(self, bucket_name: str, dict: dict, key: str):
        """
        Given a bucket name, uploads a dictionary to that bucket at the path given by the key argument
        """
        return self.client.put_object(Bucket=bucket_name, Key=key, Body=json.dumps(dict).encode('utf-8'))

    def putDataFrame(self, bucket_name: str, dataframe: pd.DataFrame, key: str) -> dict:
        """
        Given a bucket name, uploads a DataFrame to that bucket at the path given by the key argument
        """
        return self.client.put_object(Bucket=bucket_name, Key=key, Body=dataframe.to_csv().encode('utf-8'))

    def getDataFrame(self, bucket_name: str, key: str) -> pd.DataFrame:
        """
        Given a bucket name, retrieves a DataFrame from that bucket at the path given by the key argument
        """
        object = self.client.get_object(Bucket=bucket_name, Key=key)
        return pd.read_csv(io.StringIO(object['Body'].read().decode('utf-8')))

    def getCSV(self, bucket_name, key: str) -> str:
        object = self.client.get_object(Bucket=bucket_name, Key=key)
        return object['Body'].read().decode('utf-8')

    def getJSON(self, bucket_name, key: str) -> str:
        object = self.client.get_object(Bucket=bucket_name, Key=key)
        return json.loads(object['Body'].read())
    def getAllObjects(self,bucket_name):
        return self.resource.Bucket(bucket_name).objects
