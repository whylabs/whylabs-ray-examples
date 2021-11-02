import time
from functools import reduce
from typing import List

import modin.pandas as pd
import ray
from whylogs.core.datasetprofile import DatasetProfile

data_files = ["data/data1.csv", "data/data2.csv", "data/data3.csv"]
data_files = ["data/short-data.csv"]


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


def log_frame(df: pd.DataFrame) -> DatasetProfile:
    profile = DatasetProfile("")
    profile.track_dataframe(df)
    return profile


@timer("Serial")
def run_serial() -> List[str]:
    pipeline = ray.data.read_csv(data_files).window()

    results = [log_frame(batch) for batch in pipeline.iter_batches(
        batch_size=10000, batch_format="pandas")]

    merge_and_write_profiles(results, "serial.bin")


def merge_and_write_profiles(profiles: List[DatasetProfile], file_name: str):
    profile = reduce(lambda acc, cur: acc.merge(cur),
                     profiles, DatasetProfile(""))

    profile.write_protobuf(file_name)


if __name__ == "__main__":
    ray.init()
    run_serial()
