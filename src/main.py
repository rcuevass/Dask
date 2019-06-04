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


def create_and_save_artificial_data():
    df = utilities.data_frame_random_individuals()
    df.to_csv('../data/input/artificially_created_data.csv')


if __name__ == '__main__':
    initial_time = timer()
    create_and_save_artificial_data()
    final_time = timer()
    total_time = final_time - initial_time
    log.info('Total processing time, seconds=%d', total_time)
