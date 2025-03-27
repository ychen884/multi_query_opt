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

    # Check q01..q22
    for i in range(1, 23):
        q_name = f"q{i:02d}"
        expected_file = os.path.join(expected_dir, f"{q_name}_result.txt")
        actual_file = os.path.join(actual_dir, f"{q_name}_result.txt")

        if not os.path.exists(expected_file) or not os.path.exists(actual_file):
            mismatch_queries.append(q_name)
            continue

        with open(expected_file, "r") as ef, open(actual_file, "r") as af:
            expected_lines = ef.readlines()
            actual_lines = af.readlines()

        if expected_lines != actual_lines:
            mismatch_queries.append(q_name)

    if mismatch_queries:
        print("[MISMATCH] The following queries differ or have missing files:")
        for q in mismatch_queries:
            print(f"  - {q}")
        print(f"{len(mismatch_queries)} queries mismatched.")
    else:
        print("[MATCH] Correctness check passed!")

if __name__ == "__main__":
    main()
