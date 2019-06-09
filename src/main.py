import utilities
import os
import logging
from logging.handlers import TimedRotatingFileHandler
from timeit import default_timer as timer

from dask.distributed import Client, LocalCluster

from dask_ml.datasets import make_classification
from dask_ml.model_selection import train_test_split

import dask_xgboost
import xgboost
from sklearn.metrics import auc
from sklearn import metrics


log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s,%(msecs)3d %(levelname)-8s [%(filename)s:%(lineno)d] - %(message)s')

ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
ch.setFormatter(formatter)

fh = TimedRotatingFileHandler('../logs/' + os.path.basename(__file__) + '.log', when='midnight', interval=1)
fh.suffix = '%Y%m%d'
fh.setLevel(logging.DEBUG)
fh.setFormatter(formatter)

log.addHandler(fh)
log.addHandler(ch)


log.info('Executing code ...')


if __name__ == '__main__':
    #
    # Per Dask documentation
    # To set up a local cluster on your machine by instantiating a Dask Client with no arguments
    # This sets up a scheduler in your local process and several processes running single-threaded Workers
    # One can navigate to http://localhost:8787/status to see the diagnostic dashboard if you have Bokeh installed.
    client = Client('localhost:8786')
    log.info('Dask client is working now, visit=%s', 'http://localhost:8786/status')

    # set path to reduced csv, only for 2005
    path_to_reduced_csv = '../data/input/Checkouts_By_Title_Data_Lens_2005.csv'
    #
    # timing data read with pandas
    t1 = timer()
    df_pandas = utilities.read_reduced_csv_with_pandas_and_create_month(path_to_reduced_csv, log)
    t2 = timer()
    log.info('Total time processing with pandas, seconds=%d', t2 - t1)

    # timing data read with dask
    t3 = timer()
    df_dask = utilities.read_reduced_csv_with_dask_and_create_month(path_to_reduced_csv, log)
    t4 = timer()
    log.info('Total time processing with Dask, seconds=%d', t4 - t3)

    #
    # set path to whole csv
    path_to_whole_csv = '../data/input/Checkouts_By_Title.csv'
    #
    # timing data read with pandas
    t1 = timer()
    df_pandas = utilities.read_csv_with_pandas_and_count_checkouts(path_to_whole_csv, log)
    t2 = timer()
    log.info('Total time processing with pandas, seconds=%d', t2 - t1)

    # timing data read with dask
    t3 = timer()
    df_dask = utilities.read_csv_with_dask_and_count_checkouts(path_to_whole_csv, log)
    t4 = timer()
    log.info('Total time processing with Dask, seconds=%d', t4 - t3)

    log.info('Creating artificial data to test XGB...')
    td1 = timer()
    X, y = make_classification(n_samples=100000, n_features=40,
                               chunks=40, n_informative=10,
                               random_state=2019)
    td2 = timer()
    log.info('Total time creating data with Dask, seconds=%d', td2 - td1)

    log.info('Doing data split data...')
    td3 = timer()

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.20)

    td4 = timer()

    log.info('Total time doing split, seconds=%d', td4 - td3)

    params = {'objective': 'binary:logistic',
              'max_depth': 4, 'eta': 0.01, 'subsample': 0.5,
              'min_child_weight': 0.5}

    log.info("Training model...")
    td5 = timer()
    bst = dask_xgboost.train(client, params, X_train, y_train, num_boost_round=4)
    td6 = timer()
    log.info('Total time of training, seconds=%d', td6 - td5)

    log.info("Making predictions ... ")
    td7 = timer()
    y_hat = dask_xgboost.predict(client, bst, X_test).persist()
    td8 = timer()
    log.info('Total time of making predictions, seconds=%d', td8 - td7)

    fpr, tpr, thresholds = metrics.roc_curve(y_test, y_hat)
    auc_value = metrics.auc(fpr, tpr)
    log.info('Show the performance of XGB, auc=%d', auc_value)







