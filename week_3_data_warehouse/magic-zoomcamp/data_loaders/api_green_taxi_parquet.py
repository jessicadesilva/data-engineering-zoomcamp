import io
import pandas as pd
import requests
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_api(*args, **kwargs):
    """
    Template for loading data from API
    """
    df_list = []
    for i in range(1, 10):
        url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-0{i}.parquet'
        df_list.append(pd.read_parquet(url))
    for i in range(10, 13):
        url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-{i}.parquet'
        df_list.append(pd.read_parquet(url))

    return pd.concat(df_list)


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
