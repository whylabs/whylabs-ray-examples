
import os
import time
from functools import reduce
from typing import List

import modin.pandas as pd
import ray
from whylogs.app import Session
from whylogs.app.writers import WhyLabsWriter
from whylogs.core.datasetprofile import DatasetProfile

os.environ["WHYLABS_DEFAULT_ORG_ID"] = "org-3543"
data_files = ["data/data1.csv", "data/data2.csv", "data/data3.csv"]
data_files = ["data/data1.csv"]
# data_files = ["data/short-data.csv"]


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


def log_frame(df: pd.DataFrame) -> List[bytes]:
    writer = WhyLabsWriter()
    session = Session(project="demo-project",
                      pipeline="demo-pipeline", writers=[writer])
    logger = session.logger("")
    logger.log_dataframe(df)

    return logger.profile.to_protobuf().SerializeToString(deterministic=True)


@timer("Serial")
def run_serial() -> List[str]:
    pipeline = ray.data.read_csv(data_files).pipeline()

    results = [log_frame(batch) for batch in pipeline.iter_batches(
        batch_size=10000, batch_format="pandas")]

    merge_and_write_profiles(results, "serial.bin")


def merge_and_write_profiles(profiles: List[bytes], file_name: str):
    profiles = map(DatasetProfile.from_protobuf_string,  profiles)
    profile = reduce(lambda acc, cur: acc.merge(cur),
                     profiles, DatasetProfile(""))

    profile.write_protobuf(file_name)


if __name__ == "__main__":
    ray.init()
    run_serial()
