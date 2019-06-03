import utilities


def create_and_save_artificial_data():
    df = utilities.data_frame_random_individuals()
    df.to_csv('../data/input/artificially_created_data.csv')


if __name__ == '__main__':
    create_and_save_artificial_data()
