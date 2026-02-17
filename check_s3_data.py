#!/usr/bin/env python3
"""Check what data exists in S3/MinIO for Union-Find graphs."""
import boto3
from botocore.client import Config
import argparse


def list_objects(endpoint, access_key, secret_key, bucket, prefix=""):
    s3 = boto3.client(
        's3',
        endpoint_url=f"http://{endpoint}" if not endpoint.startswith("http") else endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(signature_version='s3v4'),
        region_name='us-east-1'
    )

    print(f"Listing objects in bucket '{bucket}' with prefix '{prefix}':\n")

    try:
        response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)

        if 'Contents' not in response:
            print("No objects found!")
            return

        objects = response['Contents']
        print(f"Found {len(objects)} objects:\n")

        for obj in sorted(objects, key=lambda x: x['Key']):
            size_kb = obj['Size'] / 1024
            print(f"  {obj['Key']} ({size_kb:.1f} KB)")

    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Check S3/MinIO data for Union-Find")
    parser.add_argument("--endpoint", default="localhost:9000", help="S3 endpoint")
    parser.add_argument("--access-key", default="minioadmin")
    parser.add_argument("--secret-key", default="minioadmin")
    parser.add_argument("--bucket", default="test-bucket")
    parser.add_argument("--prefix", default="uf-graphs/", help="Key prefix to list")

    args = parser.parse_args()

    list_objects(args.endpoint, args.access_key, args.secret_key, args.bucket, args.prefix)
