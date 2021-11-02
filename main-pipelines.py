import time
from functools import reduce
from typing import List

import ray
from ray.data.dataset_pipeline import DatasetPipeline
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
class RemotePipelineActor:
    def __init__(self, pipeline: DatasetPipeline) -> None:
        self.pipeline = pipeline

    def log_from_pipeline(self) -> DatasetProfile:
        profile = DatasetProfile("")
        for df in self.pipeline.iter_batches(batch_size=10000, batch_format="pandas"):
            profile.track_dataframe(df)
        return profile


@timer("Pipeline Split")
def main_pipeline_iter():
    pipelines = ray.data.read_csv(data_files).window().split(8)
    actors = [RemotePipelineActor.remote(pipeline) for pipeline in pipelines]
    results = ray.get([actor.log_from_pipeline.remote() for actor in actors])
    merge_and_write_profiles(results, "actor-pipeline.bin")


def merge_and_write_profiles(profiles: List[DatasetProfile], file_name: str):
    profile = reduce(lambda acc, cur: acc.merge(cur),
                     profiles, DatasetProfile(""))
    profile.write_protobuf(file_name)


if __name__ == "__main__":
    ray.init()
    main_pipeline_iter()
