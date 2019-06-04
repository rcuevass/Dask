import utilities
import os
import logging
from logging.handlers import TimedRotatingFileHandler
from timeit import default_timer as timer

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

log.info('Starting')


def create_and_save_artificial_data(path_to_csv,
                                    number_records=1500000):
    df = utilities.data_frame_random_individuals(number_records)
    df.to_csv(path_to_csv)


if __name__ == '__main__':
    #
    # set path to csv
    path_to_csv = '../data/input/artificially_created_data.csv'
    #
    # Create and save csv file
    t1_csv = timer()
    create_and_save_artificial_data(path_to_csv)
    t2_csv = timer()
    csv_time = t2_csv - t1_csv
    log.info('Total time creating and saving csv file, seconds=%d', csv_time)
    #
    # timing data read with pandas
    t1_csv_pandas = timer()
    df_pandas = utilities.read_csv_with_pandas(path_to_csv)
    log.info('Records in data frame = %s', df_pandas.shape[0])
    df_pandas = df_pandas.groupby('name')['surname'].sum()
    log.info('Records in data frame after grouping by = %s', df_pandas.shape[0])
    t2_csv_pandas = timer()
    log.info('Total time reading and grouping by with pandas, seconds=%d', t2_csv_pandas - t1_csv_pandas)
    #
    # timing data read with dask
    t1_csv_dask = timer()
    df_dask = utilities.read_csv_with_dask(path_to_csv)
    log.info('Records in data frame = %s', len(df_dask))
    df_dask = df_dask.groupby('name').agg({'surname': 'sum'})
    log.info('Records in data frame after grouping by = %s', len(df_dask))
    t2_csv_dask = timer()
    log.info('Total time reading and grouping by with Dask, seconds=%d', t2_csv_dask - t1_csv_dask)


