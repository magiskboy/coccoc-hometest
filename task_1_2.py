import csv
from collections import Counter


def coroutine(f):
    def decorator(*args, **kwargs):
        co = f(*args, **kwargs)
        co.send(None)  # start coroutine before it's used
        return co

    return decorator


@coroutine
def broadcast(targets):
    try:
        while 1:
            data = yield
            for target in targets:
                target.send(data)
    except GeneratorExit:
        for target in targets:
            target.close()


@coroutine
def normalize(next_):
    try:
        while 1:
            row = yield
            row["categories"] = list(map(int, row["categories"][1:-1].split(",")))
            row["counts"] = list(map(int, row["counts"][1:-1].split(",")))
            next_.send(row)
    except GeneratorExit:
        next_.close()


@coroutine
def f1(next_):
    """
    1. What is the most popular category for this sample ?
    2. Which category has the largest appeared times ?
    Answer: What's different between #1 and #2 ???

    3. Is there any idea how to represent/visualize the sample data for analysing ?
    Answer: I think we should use a bar chart for visualize the sample data
    """
    hash_map = {}
    try:
        while 1:
            row = yield
            for category, count in zip(row["categories"], row["counts"]):
                if category not in hash_map:
                    hash_map[category] = count
                else:
                    hash_map[category] += count
    except GeneratorExit:
        largest_appeared_times = 0
        largest_appeared_category = 0
        for category, times in hash_map.items():
            if largest_appeared_times < times:
                largest_appeared_times = times
                largest_appeared_category = category
        next_.send((largest_appeared_category, largest_appeared_times))
        next_.close()


@coroutine
def f2(next_):
    """
    1. Calculate the frequency for each category in the sample.
    2. From the calculation above, what do you think about the data sample ?
    Answer: I see that the sample has 4 freq class:
        - 1 - 9
        - 10 - 97
        - 100 - 999
        - 1k - 80k
    """
    counter = Counter()
    try:
        while 1:
            row = yield
            counter.update(row["categories"])
    except GeneratorExit:
        next_.send(counter)
        next_.close()


@coroutine
def printer():
    try:
        while 1:
            data = yield
            print(data)
    except GeneratorExit:
        pass


if __name__ == "__main__":
    processor = normalize(
        broadcast(
            [
                f1(printer()),
                f2(printer()),
            ]
        )
    )

    with open("data.csv", "r") as fi:
        reader = csv.DictReader(
            fi, ["object_id", "categories", "counts"], delimiter="\t"
        )
        for row in reader:
            processor.send(row)
