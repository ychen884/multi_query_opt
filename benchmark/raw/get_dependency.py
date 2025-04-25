import os

directory = "./tpc-h/models/tpch_queries/"
files = []

for file in os.listdir(directory):
    files.append(file)

files.sort()

for file in files:
    query_file = open(directory + file, "r")
    dependencies = []
    for line in query_file:
        line = line.lstrip().rstrip()
        if "source('tpch'" in line:
            index = line.find("source('tpch'")
            line = line[index:]
            tokens = line.split(" }}")
            dependencies.append(tokens[0])
    
    print(f"  - name: tpch_q{file[1:3]}")
    print(f"    description: \"TPC-H Query {file[1:3]}\"")
    print(f"    depends_on:")
    for dependency in dependencies:
        print(f"      - {dependency}")

    print()