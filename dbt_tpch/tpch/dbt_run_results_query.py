#!/usr/bin/env python3
import os
import duckdb

def main():
    db_path = "dev.duckdb"

    # connect to an existing DuckDB file WITHOUT re-initializing data
    con = duckdb.connect(db_path)

    os.makedirs("expected_results", exist_ok=True)

    # for each q01..q22, read the view content and save to a text file
    for i in range(1, 23):
        q_name = f"q{i:02d}"
        out_filename = f"./expected_results/{q_name}_result.txt"

        select_stmt = f'SELECT * FROM "dev"."main"."{q_name}"'
        print(f"[INFO] Fetching {q_name} -> writing to {out_filename}")

        try:
            results = con.execute(select_stmt).fetchall()
            with open(out_filename, "w") as out_file:
                if results:
                    for row in results:
                        out_file.write(str(row) + "\n")
                else:
                    out_file.write("[No rows returned]\n")
        except Exception as e:
            print(f"[ERROR selecting from {q_name}]: {e}")
            with open(out_filename, "w") as out_file:
                out_file.write(f"[ERROR selecting from {q_name}]: {e}")

    con.close()
    print("[INFO] All expected results written under `expected_results/`")

if __name__ == "__main__":
    main()
