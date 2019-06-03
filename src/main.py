import utilities


def main():
    df = utilities.data_frame_random_individuals()
    return df


if __name__ == '__main__':
    df = main()
    print(df.head(10))