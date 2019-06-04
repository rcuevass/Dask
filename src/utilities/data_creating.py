import pandas as pd
import random


def generate_random_person(names_list, surnames_list, salaries_list):
    return {"name": random.sample(names_list, 1)[0],
            "surname": random.sample(surnames_list, 1)[0],
            "salary": random.sample(salaries_list, 1)[0]}


def generate_individuals(k,
                         names_list = ["Roger", "John", "Xico", "Henry", "Mike"],
                         surnames_list = ["Goodman", "Feynman", "White", "Red", "Cave"],
                         salaries_list = [250*random.randint(10, 30) for _ in range(10)]):
    return [generate_random_person(names_list, surnames_list, salaries_list) for _ in range(k)]


def data_frame_random_individuals(number_individuals=250000):
    df = pd.DataFrame(generate_individuals(k=number_individuals),
                      columns=["name", "surname", "salary"])
    return df
