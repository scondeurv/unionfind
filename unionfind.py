#!/usr/bin/env python3
"""
Union-Find (Connected Components) - Burst Execution

This script orchestrates the distributed Union-Find algorithm execution
on OpenWhisk using the burst communication middleware.
"""

import argparse
import json
import sys
import os

# Add parent directory to path to import ow_client
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ow_client.parser import add_openwhisk_to_parser, add_burst_to_parser, try_or_except
from ow_client.time_helper import get_millis
from ow_client.openwhisk_executor import OpenwhiskExecutor
from unionfind_utils import generate_payload, add_unionfind_to_parser


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run distributed Union-Find on OpenWhisk")
    add_openwhisk_to_parser(parser)
    add_unionfind_to_parser(parser)
    add_burst_to_parser(parser)
    args = try_or_except(parser)

    params = generate_payload(
        endpoint=args.uf_endpoint,
        partitions=args.partitions,
        num_nodes=args.num_nodes,
        bucket=args.bucket,
        key=args.key,
        max_iterations=args.max_iterations,
        granularity=args.granularity
    )

    executor = OpenwhiskExecutor(args.ow_host, args.ow_port, args.debug)

    print(f"Launching Union-Find with {len(params)} workers...")
    print(f"  - Nodes: {args.num_nodes}")
    print(f"  - Partitions: {args.partitions}")
    print(f"  - Granularity: {args.granularity}")

    host_submit = get_millis()
    dt = executor.burst("unionfind",
                        params,
                        file="./unionfind.zip",
                        memory=args.runtime_memory if args.runtime_memory else 2048,
                        custom_image=args.custom_image,
                        debug_mode=args.debug,
                        burst_size=1,
                        join=args.join,
                        backend=args.backend,
                        chunk_size=args.chunk_size,
                        is_zip=True,
                        timeout=300000)
    finished = get_millis()

    dt_results = dt.get_results()
    
    # Flatten results from all workers
    flattened_results = []
    for sublist in dt_results:
        if isinstance(sublist, list):
            flattened_results.extend(sublist)
        else:
            print(f"WARNING: Unexpected result structure: {sublist}")
            continue

    if not flattened_results:
        print("ERROR: No results returned from workers.")
        sys.exit(1)

    flattened_results.sort(key=lambda x: x.get('key', 'unknown'))

    # Extract and display results
    results = []
    num_components = None
    for i in flattened_results:
        res = {
            "fn_id": i.get("key", "unknown"),
            "host_submit": host_submit,
            "timestamps": i.get("timestamps", []),
            "finished": finished
        }
        if "num_components" in i and i["num_components"] is not None:
            num_components = i["num_components"]
            res["num_components"] = num_components
        if "results" in i and i["results"] is not None:
            print(i["results"])
        results.append(res)

    # Save results
    output_file = "unionfind-burst.json"
    with open(output_file, "w") as f:
        json.dump(results, f, indent=2)

    print(f"\nResults saved to {output_file}")
    if num_components is not None:
        print(f"Connected components found: {num_components}")
    print(f"Total execution time: {finished - host_submit} ms")
