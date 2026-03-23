import argparse
import json
import os

DEFAULT_OUTPUT = "unionfind_payload.json"

AWS_S3_REGION = "us-east-1"
AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin")


def generate_payload(endpoint, partitions, num_nodes, bucket, key, max_iterations=None, granularity=1, input_format="binary"):
    """Generate payload for Union-Find workers."""
    payload_list = []
    num_requests = partitions // granularity
    
    for i in range(num_requests):
        payload_list.append(
            {
                "group_id": i,
                "partitions": partitions,
                "granularity": granularity,
                "num_nodes": num_nodes,
                **({"max_iterations": max_iterations} if max_iterations is not None else {}),
                "input_data": {
                    "bucket": bucket,
                    "key": key,
                    "format": input_format,
                    "endpoint": endpoint,
                    "region": AWS_S3_REGION,
                    "aws_access_key_id": AWS_ACCESS_KEY_ID,
                    "aws_secret_access_key": AWS_SECRET_ACCESS_KEY,
                },
            }
        )

    return payload_list


def add_unionfind_to_parser(parser):
    """Add Union-Find specific arguments to the argument parser."""
    parser.add_argument("--uf-endpoint", type=str, required=True,
                        help="Endpoint of the S3 service where the graph is stored")
    parser.add_argument("--partitions", type=int, required=True,
                        help="Number of partitions")
    parser.add_argument("--num-nodes", type=int, required=True,
                        help="Number of nodes in the graph")
    parser.add_argument("--bucket", type=str, required=True,
                        help="S3 bucket name")
    parser.add_argument("--key", type=str, required=True,
                        help="S3 object key (base path)")
    parser.add_argument("--max-iterations", type=int, default=None,
                        help="Maximum iterations (default 100)")
    parser.add_argument("--input-format", type=str, default="binary", choices=["binary", "tsv", "text"],
                        help="Input partition format in S3 (default: binary)")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate Union-Find payload")
    add_unionfind_to_parser(parser)
    parser.add_argument("--granularity", type=int, default=1,
                        help="Partitions per worker (default 1)")
    parser.add_argument("--output", type=str, default=DEFAULT_OUTPUT,
                        help=f"Output file (default {DEFAULT_OUTPUT})")
    args = parser.parse_args()

    payload = generate_payload(
        endpoint=args.uf_endpoint,
        partitions=args.partitions,
        num_nodes=args.num_nodes,
        bucket=args.bucket,
        key=args.key,
        max_iterations=args.max_iterations,
        granularity=args.granularity,
        input_format=args.input_format
    )

    with open(args.output, "w") as f:
        json.dump(payload, f, indent=2)
    
    print(f"Generated payload with {len(payload)} workers to {args.output}")
