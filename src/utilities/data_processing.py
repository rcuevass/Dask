import pandas as pd
import dask.dataframe as ddf
from timeit import default_timer as timer


def read_csv_with_pandas_and_create_month(path_to_csv, log):
    t11 = timer()
    df = pd.read_csv(path_to_csv,
                     dtype={'BibNumber': 'str',
                            'ItemBarcode': 'str',
                            'ItemType': 'str',
                            'Collection': 'str',
                            'CallNumber': 'str',
                            'CheckoutDateTime': 'str'},
                     low_memory=False)
    t12 = timer()
    log.info('Number of records=%s', df.shape[0])
    log.info('Time reading csv with pandas, seconds=%d', t12 - t11)
    log.info('Number of records before processing=%s', df.shape[0])
    df['Month'] = df['CheckoutDateTime'].apply(lambda x: str(x[:2]))
    t13 = timer()
    log.info('Time creating month column with pandas, seconds=%d', t13 - t12)
    df = df.groupby('Month')['Month'].count()
    t14 = timer()
    log.info('Time grouping by and summing with pandas, seconds=%d', t14 - t13)
    log.info('Number of records after processing=%s', df.shape[0])
    return df


def read_csv_with_dask_and_create_month(path_to_csv, log):
    t21 = timer()
    df = ddf.read_csv(path_to_csv,
                      dtype={'BibNumber': 'str',
                             'ItemBarcode': 'str',
                             'ItemType': 'str',
                             'Collection': 'str',
                             'CallNumber': 'str',
                             'CheckoutDateTime': 'str'})
    t22 = timer()
    log.info('Time reading csv with Dask, seconds=%d', t22 - t21)
    log.info('Number of records before processing=%s', len(df))
    df['Month'] = df['CheckoutDateTime'].apply(lambda x: str(x[:2]), meta=('str'))
    t23 = timer()
    log.info('Time creating month column with Dask, seconds=%d', t23 - t22)
    df = df.groupby('Month').Month.count()
    t24 = timer()
    log.info('Time grouping by and summing with Dask, seconds=%d', t24 - t23)
    log.info('Number of records after processing=%s', len(df))
    return df
