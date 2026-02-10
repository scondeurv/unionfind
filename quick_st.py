import os
import time
import subprocess
import json
import boto3
from botocore.config import Config

def download_and_run(num_nodes, partitions):
    bucket = "test-bucket"
    key = f"uf-graphs/uf-{num_nodes}"
    endpoint = "http://localhost:9000"
    output_file = f"tmp_{num_nodes}.tsv"
    
    print(f"Downloading {num_nodes} nodes graph...")
    s3 = boto3.client('s3', endpoint_url=endpoint, aws_access_key_id='minioadmin', aws_secret_access_key='minioadmin', config=Config(signature_version='s3v4'), region_name='us-east-1')
    
    total_edges = 0
    with open(output_file, 'w') as f:
        for part_id in range(partitions):
            part_key = f"{key}/part-{part_id:05d}"
            response = s3.get_object(Bucket=bucket, Key=part_key)
            content = response['Body'].read().decode('utf-8')
            f.write(content)
            total_edges += len(content.strip().split('\n'))

    print(f"Downloaded {total_edges} edges. Running standalone...")
    start = time.time()
    res = subprocess.run(["./ufst/target/release/union-find", output_file, str(num_nodes)], capture_output=True, text=True)
    duration = time.time() - start
    if res.returncode != 0:
        print(f"Standalone FAILED for {num_nodes}: {res.stderr}")
    else:
        print(f"Standalone result for {num_nodes}: {res.stdout.strip()}")
    print(f"Total time (including download + exec): {duration:.3f}s")
    os.remove(output_file)

if __name__ == "__main__":
    download_and_run(10000000, 16)
    download_and_run(25000000, 16)
