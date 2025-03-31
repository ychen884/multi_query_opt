#!/usr/bin/env python3
import os
import duckdb

def main():
    db_path = "dev.duckdb"

    # connect to an existing DuckDB file WITHOUT re-initializing data
    con = duckdb.connect(db_path)

    os.makedirs("expected_results", exist_ok=True)

    materialized_tables_file = "materialized_tables.txt"
    if not os.path.exists(materialized_tables_file):
        print(f"[ERROR] {materialized_tables_file} does not exist.")
        return

    with open(materialized_tables_file, "r") as f:
        lines = f.readlines()

    for line in lines:
        line = line.strip()
        if not line:
            continue

        # line might be something like: "dev"."main"."q06"
        table_fqn = line

        # Extract the short final name (e.g. q06) for the output file
        cleaned = table_fqn.replace('"', '')
        final_name = cleaned.split('.')[-1]  # e.g. q06

        # The output file name is <table_name>.result
        out_filename = os.path.join("expected_results", f"{final_name}.result")

        select_stmt = f"SELECT * FROM {table_fqn}"
        print(f"[INFO] Fetching {table_fqn} -> writing to {out_filename}")

        try:
            results = con.execute(select_stmt).fetchall()
            with open(out_filename, "w") as out_file:
                if results:
                    for row in results:
                        out_file.write(str(row) + "\n")
                else:
                    out_file.write("[No rows returned]\n")
        except Exception as e:
            print(f"[ERROR selecting from {table_fqn}]: {e}")
            with open(out_filename, "w") as out_file:
                out_file.write(f"[ERROR selecting from {table_fqn}]: {e}")

    con.close()
    print("[INFO] All expected results written under `expected_results/`")

if __name__ == "__main__":
    main()
