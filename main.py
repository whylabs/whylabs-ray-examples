from whylogs.proto.messages_pb2 import DatasetProfileMessage
from whylogs.core.datasetprofile import DatasetProfile
from whylogs.app.writers import WhyLabsWriter
from whylogs.app import Session
from ray.data.impl.arrow_block import ArrowRow
from ray.data.dataset_pipeline import DatasetPipeline
import os
import modin.pandas as pd
import pandas
from typing import List
import time
from functools import reduce
from re import I
from time import sleep
import ray
ray.init()


os.environ["WHYLABS_DEFAULT_ORG_ID"] = "org-3543"
data_files = ["data/data1.csv", "data/data2.csv", "data/data3.csv"]
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


class WhylogsActor:
    def __init__(self, data_frame) -> None:
        self.data_frame = data_frame
        writer = WhyLabsWriter("", formats=[])
        self.session = Session(project="demo-project",
                               pipeline="demo-pipeline", writers=[writer])

    def log(self) -> str:
        # with self.session.logger(tags={"datasetId": "model-1"}) as ylog:
        # ylog.log_dataframe(self.data_frame)
        summary = self.session.profile_dataframe(self.data_frame).to_protobuf()
        return summary.SerializeToString(deterministic=True)


RayWhylogsActor = ray.remote(WhylogsActor)


@ray.remote
class RemotePipelineActor:
    def __init__(self, pipeline: DatasetPipeline) -> None:
        self.pipeline = pipeline

    def log_from_pipeline(self) -> List[bytes]:
        writer = WhyLabsWriter("", formats=[])
        session = Session(project="demo-project",
                          pipeline="demo-pipeline", writers=[writer])
        logger = session.logger("")
        print('logging')
        for df in self.pipeline.iter_batches(batch_size=10000, batch_format="pandas"):
            logger.log_dataframe(df)

        return logger.profile.to_protobuf().SerializeToString(deterministic=True)


@timer("ActorPipeline")
def main_pipeline_actor():
    pipelines = ray.data.read_csv(data_files).pipeline().split(8)

    actors = [RemotePipelineActor.remote(pipeline) for pipeline in pipelines]
    results = ray.get([actor.log_from_pipeline.remote() for actor in actors])

    # TODO this ends up with some really scary error because the cache is evicted or something,
    # I assume it has something to do with the state of the pipelines relative to the node this
    # executes on. Using an actor instead to store the pipeline reference.
    # results = ray.get([log_from_pipeline.remote(pipeline) for pipeline in pipelines ])

    merge_and_write_profiles(results, "actor-pipeline.bin")


def aggregate_to_frame(df: pd.DataFrame) -> List[bytes]:
    writer = WhyLabsWriter("", formats=[])
    session = Session(project="demo-project",
                      pipeline="demo-pipeline", writers=[writer])
    logger = session.logger("")
    # for df in pipe.iter_batches(batch_size=100, batch_format="pandas"):
    logger.log_dataframe(df)

    return logger.profile.to_protobuf().SerializeToString(deterministic=True)


# Needed to tell modin the proper types of each column. It can't tell when it reads from csv.
# pandas_csv = pandas.read_csv(data_files[0])
@timer("Modin")
def main_modin():
    df = pd.read_csv("data/data1.csv")
    df.groupby.grou
    results = df.aggregate(aggregate_to_frame, axis=0)
    print(results)


@ray.remote
def log_frame(df: pd.DataFrame) -> List[bytes]:
    writer = WhyLabsWriter()
    session = Session(project="demo-project",
                      pipeline="demo-pipeline", writers=[writer])
    logger = session.logger("")
    # for df in pipe.iter_batches(batch_size=100, batch_format="pandas"):
    logger.log_dataframe(df)

    return logger.profile.to_protobuf().SerializeToString(deterministic=True)


@timer("IterPipeline")
def main_pipeline_iter():
    pipeline = ray.data.read_csv(data_files).pipeline(parallelism=3)

    results = ray.get([log_frame.remote(batch) for batch in pipeline.iter_batches(
        batch_size=1000, batch_format="pandas")])

    merge_and_write_profiles(results, "iter-pipeline.bin")


def run_serial() -> List[str]:
    yactors = [WhylogsActor(pd.read_csv(file_name))
               for file_name in data_files]
    return list(map(lambda actor: actor.log(), yactors))


def run_parallel() -> List[str]:
    yactors = [RayWhylogsActor.remote(pd.read_csv(file_name))
               for file_name in data_files]
    serialized_profiles_ref = list(
        map(lambda actor: actor.log.remote(), yactors))

    return ray.get(serialized_profiles_ref)


def merge_and_write_profiles(profiles: List[bytes], file_name: str):
    profiles = map(DatasetProfile.from_protobuf_string,  profiles)
    profile = reduce(lambda acc, cur: acc.merge(cur),
                     profiles, DatasetProfile(""))

    profile.write_protobuf(file_name)


@timer("Serial")
def main_test_serial():
    merge_and_write_profiles(run_serial(), "serial.bin")


@timer("Parallel")
def main_test_parallel():
    merge_and_write_profiles(run_parallel(), "parallel.bin")


if __name__ == "__main__":

    # This is the control. It doesn't use ray
    # main_test_serial()

    # This one uses ray via actors, sending one csv to each one/process
    # main_test_parallel()

    # This one uses ray via functions, sending each function the data-frame-ified csv
    # from the main process.
    main_pipeline_iter()

    # This one won't work on WSL
    # main_pipeline_actor()

    # This one uses modin as a pd replacement
    # main_modin()
