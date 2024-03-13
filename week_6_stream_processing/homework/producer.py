import pandas as pd
import datetime
import json
import time

from kafka import KafkaProducer


def json_serializer(data):
    return json.dumps(data).encode("utf-8")


if __name__ == "__main__":

    df_green = pd.read_csv("green_tripdata_2019-10.csv.gz", compression="gzip")

    df_green = df_green[
        [
            "lpep_pickup_datetime",
            "lpep_dropoff_datetime",
            "PULocationID",
            "DOLocationID",
            "passenger_count",
            "trip_distance",
            "tip_amount",
        ]
    ]

    server = "localhost:9092"
    producer = KafkaProducer(
        bootstrap_servers=[server], value_serializer=json_serializer
    )
    producer.bootstrap_connected()

    topic_name = "green-trips"
    for row in df_green.itertuples(index=False):
        row_dict = {col: getattr(row, col) for col in row._fields}
        row_dict["timestamp"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        producer.send(topic_name, value=row_dict)

    producer.flush()
