# Query Processing Over Streaming Data Using Flink

## Generate Database (TPC-H Standard)

You can call it using `./run_dbgen.sh $size`, where `$size` is the size of the database. For example, running `./run_dbgen.sh 10` will generate 10GB of data.

## Run Locally with Flink

Run `./run.sh` to compile and run the Flink code locally. The results will be saved in `output.txt`.

## Verify Correctness

Run `process_tpch_data.py` to load the data into the database and run the SQL query. The query results will be saved in `query_result.csv`.

## Run on Flink Cluster

1. Modify the absolute path of `input_data_all.csv` in `StreamProcessingJob.java` to match your local machine's path
2. Install Flink 1.20.1
3. Submit and run the job through the Flink web UI at http://localhost:8081/

### Prerequisites

- Java 8 or higher
- Flink 1.20.1
- Python 3.x 
