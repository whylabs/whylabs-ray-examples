import pandas as pd
import ray
from whylogs.core import DatasetProfile, DatasetProfileView

from util import data_files, merge_profiles, timer


# This test uses ray.remote to execute the logging on the ray cluster.
@ray.remote
def log_frame(df: pd.DataFrame) -> DatasetProfileView:
    profile = DatasetProfile()
    profile.track(df)
    return profile.view()


@timer("IterPipeline")
def main_pipeline_iter() -> DatasetProfile:
    pipeline = ray.data.read_csv(data_files).window()
    pipelines = pipeline.iter_batches(batch_size=1000, batch_format="pandas")
    results = ray.get([log_frame.remote(batch) for batch in pipelines])

    return merge_profiles(results)


if __name__ == "__main__":
    ray.init()
    main_pipeline_iter()
