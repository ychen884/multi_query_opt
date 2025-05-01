#!/usr/bin/env python3

import csv
import sys

def read_benchmark_csv(filepath):
    """
    Reads a CSV file with columns:
        TableName,TotalCreationTimeNs(10runs)
    Returns a dict {table_name: time}.
    """
    data = {}
    with open(filepath, "r", newline="") as f:
        reader = csv.reader(f)
        header = next(reader, None)  # Skip header line
        for row in reader:
            if len(row) < 2:
                continue
            table_name = row[0]
            time_str = row[1].strip()
            if not time_str:
                continue
            try:
                time_val = int(time_str)
            except ValueError:
                time_val = None
            data[table_name] = time_val
    return data

def main():
    if len(sys.argv) < 4:
        print(f"Usage: {sys.argv[0]} <unoptimized_csv> <optimized_csv> <output_csv>")
        sys.exit(1)

    unoptimized_csv = sys.argv[1]
    optimized_csv = sys.argv[2]
    output_csv = sys.argv[3]

    # Read the two input files
    unopt_data = read_benchmark_csv(unoptimized_csv)
    opt_data = read_benchmark_csv(optimized_csv)

    # Union of all table names
    all_tables = set(unopt_data.keys()) | set(opt_data.keys())

    total_unopt_time = 0
    total_opt_time = 0

    # Write merged results
    with open(output_csv, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "TableName",
            "UnoptimizedTimeMs",
            "OptimizedTimeMs",
            # "AbsoluteMsDiff (Unopt-Opt)",
            "Speedup"
        ])
        
        for table_name in sorted(all_tables):
            unopt_time = unopt_data.get(table_name)
            opt_time = opt_data.get(table_name)

            # Sum total times if available
            if isinstance(unopt_time, int) and unopt_time != -1:
                total_unopt_time += unopt_time
            if isinstance(opt_time, int) and opt_time != -1:
                total_opt_time += opt_time

            # If either is missing, store a placeholder
            if unopt_time is None or opt_time is None or unopt_time == -1 or opt_time == -1:
                abs_diff = ""
                pct_improv = ""
            else:
                # abs_diff = unopt_time - opt_time
                # # Avoid division by zero
                # if unopt_time == 0:
                #     pct_improv = ""
                # else:
                #     pct_improv = (abs_diff / unopt_time) * 100
                if opt_time == 0:
                    pct_improv = ""
                else:
                    pct_improv = unopt_time / opt_time

            writer.writerow([
                table_name,
                unopt_time if unopt_time is not None else "MISSING",
                opt_time if opt_time is not None else "MISSING",
                # abs_diff,
                pct_improv
            ])

        # Write summary totals at bottom
        writer.writerow([])
        writer.writerow([
            "TOTAL",
            total_unopt_time,
            total_opt_time,
            total_unopt_time - total_opt_time,
            f"{((total_unopt_time - total_opt_time) / total_unopt_time * 100):.2f}" if total_unopt_time != 0 else ""
        ])

    print(f"[INFO] Merged results written to {output_csv}")

if __name__ == "__main__":
    main()
