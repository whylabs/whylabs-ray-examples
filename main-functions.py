import os
import time
from functools import reduce
from typing import List

import pandas as pd
import ray
from whylogs.app import Session
from whylogs.app.writers import WhyLabsWriter
from whylogs.core.datasetprofile import DatasetProfile

os.environ["WHYLABS_DEFAULT_ORG_ID"] = "org-3543"
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
def log_frame(df: pd.DataFrame) -> List[bytes]:
    writer = WhyLabsWriter()
    session = Session(project="ignored", pipeline="ignored", writers=[writer])
    logger = session.logger("")
    logger.log_dataframe(df)

    return logger.profile.serialize_delimited()
    # return logger.profile.to_protobuf().SerializeToString(deterministic=True)


@timer("IterPipeline")
def main_pipeline_iter():
    pipeline = ray.data.read_csv(data_files).pipeline()

    pipelines = pipeline.iter_batches(batch_size=1000, batch_format="pandas")
    serialized_profiles = [log_frame.remote(batch) for batch in pipelines]
    print(len(serialized_profiles))
    results = ray.get(serialized_profiles)

    merge_and_write_profiles(results, "iter-pipeline.bin")


def merge_and_write_profiles(profiles: List[bytes], file_name: str):
    profiles = map(DatasetProfile.parse_delimited,  profiles)
    flat_profiles = [item for sublist in profiles for item in sublist]
    profile = reduce(
        lambda acc, cur: acc.merge(cur),
        flat_profiles,
        DatasetProfile(""))
    profile.write_protobuf(file_name)


if __name__ == "__main__":
    ray.init()
    main_pipeline_iter()
