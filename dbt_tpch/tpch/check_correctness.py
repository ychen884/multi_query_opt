#!/usr/bin/env python3
import sys
import os

def main():
    # expect a single argument: either "optimized" or "not_optimized"
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <optimized|not_optimized>")
        sys.exit(1)

    mode = sys.argv[1]
    if mode not in ["optimized", "not_optimized"]:
        print(f"[ERROR] Invalid argument: {mode}")
        print("Please pass either 'optimized' or 'not_optimized'.")
        sys.exit(1)

    # Compare the expected_results vs. <mode>_results
    expected_dir = "./expected_results"
    actual_dir = f"./{mode}_results"

    mismatch_queries = []

    # 1) Read materialized_tables.txt
    materialized_tables_file = "materialized_tables.txt"
    if not os.path.exists(materialized_tables_file):
        print(f"[ERROR] {materialized_tables_file} does not exist.")
        sys.exit(1)

    with open(materialized_tables_file, "r") as f:
        lines = f.readlines()

    for line in lines:
        line = line.strip()
        if not line:
            continue
        # line example:  "dev"."main"."q06"
        # remove quotes and split by dot to get the final segment (e.g. q06)
        cleaned = line.replace('"', '')
        final_name = cleaned.split('.')[-1]

        # Our script from earlier wrote results into <final_name>.result or <final_name>_result.txt
        # Suppose your pattern is <final_name>_result.txt. Adjust if needed.
        expected_file = os.path.join(expected_dir, f"{final_name}.result")
        actual_file   = os.path.join(actual_dir, f"{final_name}.result")

        if not (os.path.exists(expected_file) and os.path.exists(actual_file)):
            mismatch_queries.append(final_name)
            continue

        # Compare file contents line by line
        with open(expected_file, "r") as ef, open(actual_file, "r") as af:
            expected_lines = ef.readlines()
            actual_lines = af.readlines()

        if expected_lines != actual_lines:
            mismatch_queries.append(final_name)

    if mismatch_queries:
        print("[MISMATCH] The following queries differ or have missing files:")
        for q in mismatch_queries:
            print(f"  - {q}")
        print(f"{len(mismatch_queries)} queries mismatched.")
    else:
        print("[MATCH] Correctness check passed!")

if __name__ == "__main__":
    main()
