import ray
import time
from ray.data.dataset_pipeline import DatasetPipeline
from whylogs.core import DatasetProfile, DatasetProfileView

from util import merge_profiles, timer, data_files


@ray.remote
class RemotePipelineActor:
    def __init__(self, pipeline: DatasetPipeline) -> None:
        self.pipeline = pipeline

    def log_from_pipeline(self) -> DatasetProfileView:
        profile = DatasetProfile()
        # TODO jamie looks like splitting the files up still makes tracking a lot faster
        for df in self.pipeline.iter_batches(batch_size=10000, batch_format="pandas"):
            pass
            profile.track(pandas=df)
        return profile.view()


@timer("Pipeline Split")
def main_pipeline_iter():
    pipelines = ray.data.read_csv(data_files).window(
        blocks_per_window=8).split(8)
    actors = [RemotePipelineActor.remote(pipeline) for pipeline in pipelines]
    results = ray.get([actor.log_from_pipeline.remote() for actor in actors])

    merged = merge_profiles(results)
    merged.write("actor-pipeline.bin")


if __name__ == "__main__":
    ray.init()
    main_pipeline_iter()
