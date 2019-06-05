import pandas as pd
from timeit import default_timer as timer

import dask.dataframe as ddf
from dask.distributed import Client, LocalCluster

cluster = LocalCluster(processes=False) # to use threads instead
client = Client(cluster)


def read_reduced_csv_with_pandas_and_create_month(path_to_csv, log):
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


def read_reduced_csv_with_dask_and_create_month(path_to_csv, log):
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


def read_csv_with_pandas_and_count_checkouts(path_to_csv, log):
    t11 = timer()
    df = pd.read_csv(path_to_csv,
                     dtype={'UsageClass': 'str',
                            'CheckoutType': 'str',
                            'MaterialType': 'str',
                            'CheckoutYear': 'str',
                            'CheckoutMonth': 'str',
                            'Checkouts': 'float',
                            'Title': 'str',
                            'Creator': 'str',
                            'Subjects': 'str',
                            'Publisher': 'str',
                            'PublicationYear': 'str'},
                     low_memory=False)
    t12 = timer()
    log.info('Time reading csv with pandas, seconds=%d', t12 - t11)
    log.info('Number of records before processing=%s', df.shape[0])
    #
    # keep columns of interest only
    df = df[['UsageClass', 'Title', 'CheckoutYear', 'Checkouts']]
    #
    # Replace missing checkout count with 0
    df['Checkouts'] = df['Checkouts'].fillna(0)
    #
    # create a column that concatenates usage class and checkout year
    df['class_year'] = df[['UsageClass', 'CheckoutYear']].apply(lambda x: str(x[0]) + '-' + str(x[1]), axis=1)
    t13 = timer()
    log.info('Time creating class_title_year with pandas, seconds=%d', t13 - t12)
    df = df.groupby('class_year')['Checkouts'].agg('sum')
    t14 = timer()
    log.info('Time grouping by and summing with pandas, seconds=%d', t14 - t13)
    log.info('Number of records after processing=%s', df.shape[0])
    return df


def read_csv_with_dask_and_count_checkouts(path_to_csv, log):
    t21 = timer()
    df = ddf.read_csv(path_to_csv,
                      dtype={'UsageClass': 'str',
                             'CheckoutType': 'str',
                             'MaterialType': 'str',
                             'CheckoutYear': 'str',
                             'CheckoutMonth': 'str',
                             'Checkouts': 'float',
                             'Title': 'str',
                             'Creator': 'str',
                             'Subjects': 'str',
                             'Publisher': 'str',
                             'PublicationYear': 'str'}
                      )
    t22 = timer()
    log.info('Time reading csv with Dask, seconds=%d', t22 - t21)
    log.info('Number of records before processing=%s', len(df))
    #
    # keep columns of interest only
    df = df[['UsageClass', 'Title', 'CheckoutYear', 'Checkouts']]
    #
    # create a column that concatenates three columns
    df['class_year'] = df[['UsageClass', 'CheckoutYear']].apply(lambda x: str(x[0]) + '-' + str(x[1]),
                                                                meta=('str'),
                                                                axis=1)
    t23 = timer()
    log.info('Time creating month column with Dask, seconds=%d', t23 - t22)
    df = df.groupby('class_year').Checkouts.sum()
    t24 = timer()
    log.info('Time grouping by and summing with Dask, seconds=%d', t24 - t23)
    log.info('Number of records after processing=%s', len(df))
    return df

