import pandas as pd
from sqlalchemy import create_engine
from time import time
import argparse
import os


def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url

    csv_name = "output.csv"

    if url[-2:] == "gz":
        file_name = url.split("/")[-1]
        # os.system(f"wget {url} -O {file_name}")
        # os.system(f"gunzip -c {file_name} > {csv_name}")
        os.system(f"wget {url} -O {file_name}")
    else:
        os.system(f"wget {url} -O {csv_name}")

    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")

    engine.connect()

    df_iter = pd.read_csv(
        file_name,
        iterator=True,
        chunksize=100000,
    )

    count = 0
    for df in df_iter:
        for col in df.columns:
            if "datetime" in col:
                df[col] = pd.to_datetime(df[col])
        t_start = time()
        if count == 0:
            df.to_sql(table_name, con=engine, if_exists="replace")
        else:
            df.to_sql(table_name, con=engine, if_exists="append")
        t_end = time()
        print(f"Inserted another chunk..., took {t_end - t_start:.3f} seconds")
        count += 1


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest CSV data to Postgres")

    parser.add_argument("--user", help="user name for postgres")
    parser.add_argument("--password", help="password for postgres")
    parser.add_argument("--host", help="host for postgres")
    parser.add_argument("--port", help="port for postgres")
    parser.add_argument("--db", help="database name for postgres")
    parser.add_argument("--table_name", help="name of table results will be written to")
    parser.add_argument("--url", help="url of the csv file")

    args = parser.parse_args()

    main(args)
