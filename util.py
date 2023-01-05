import time
from datetime import datetime
from functools import reduce
from typing import List

from ray.data.dataset_pipeline import DatasetPipeline
from whylogs.core import DatasetProfile, DatasetProfileView

data_files = ["data/data1.csv", "data/data2.csv", "data/data3.csv"]
# Same files repeated many times. Makes the perf increases from Ray much more noticeable.
lots_of_data_files = ["data/data1.csv", "data/data2.csv", "data/data3.csv", "data/data1.csv", "data/data2.csv", "data/data3.csv", "data/data1.csv", "data/data2.csv",
                      "data/data3.csv", "data/data1.csv", "data/data2.csv", "data/data3.csv", "data/data1.csv", "data/data2.csv", "data/data3.csv", "data/data1.csv", "data/data2.csv", "data/data3.csv"]
short_data_files = ["data/short-data.csv"]


def merge_profiles(profiles: List[DatasetProfileView]) -> DatasetProfileView:
    view = DatasetProfileView(
        columns={},
        dataset_timestamp=datetime.now(),
        creation_timestamp=datetime.now())

    return reduce(lambda acc, cur:  acc.merge(cur), profiles, view)


def timer(name):
    def wrapped(fn):
        def timerfn():
            print(f"========== {name} =============")
            serial_start = time.time()
            fn()
            print(f"time {time.time() - serial_start} seconds")
            print()
        return timerfn
    return wrapped
