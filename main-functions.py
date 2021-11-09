import time
from functools import reduce

import pandas as pd
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


@ray.remote
def log_frame(df: pd.DataFrame) -> DatasetProfile:
    profile = DatasetProfile("")
    profile.track_dataframe(df)
    return profile


@timer("IterPipeline")
def main_pipeline_iter() -> DatasetProfile:
    pipeline = ray.data.read_csv(data_files).window()
    pipelines = pipeline.iter_batches(batch_size=1000, batch_format="pandas")
    results = ray.get([log_frame.remote(batch) for batch in pipelines])
    profile = reduce(
        lambda acc, cur: acc.merge(cur),
        results,
        DatasetProfile(""))
    return profile


if __name__ == "__main__":
    ray.init()
    main_pipeline_iter()
