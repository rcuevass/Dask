import pandas as pd
import dask.dataframe as ddf


def read_csv_with_pandas(path_to_csv):
    df = pd.read_csv(path_to_csv)
    return df


def read_csv_with_dask(path_to_csv):
    df = ddf.read_csv(path_to_csv)
    return df
