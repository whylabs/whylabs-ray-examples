import pandas as pd
import ray
from whylogs.core import DatasetProfile, DatasetProfileView

from util import data_files, merge_profiles, timer

# This doesn't use ray for the logging, just for the data reads. This file
# should perform the worst of all.

def log_frame(df: pd.DataFrame) -> DatasetProfileView:
    profile = DatasetProfile()
    profile.track(df)
    return profile.view()


@timer("Serial (no ray)")
def run_serial():
    pipeline = ray.data.read_csv(data_files).window()

    iter = pipeline.iter_batches(
        batch_size=10000, batch_format="pandas")
    results = [log_frame(batch) for batch in iter]

    profile = merge_profiles(results)
    profile.write("serial.bin")
    return profile


if __name__ == "__main__":
    ray.init()
    run_serial()
