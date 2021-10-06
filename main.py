from functools import reduce
from time import sleep
import ray
import time
from typing import List

import pandas as pd
import os
from whylogs.app import Session
from whylogs.app.writers import WhyLabsWriter
from whylogs.core.datasetprofile import DatasetProfile
from whylogs.proto.messages_pb2 import DatasetProfileMessage

os.environ["WHYLABS_DEFAULT_ORG_ID"] = "org-3543"
data_files = ["data/data1.csv", "data/data2.csv", "data/data3.csv"]
# data_files = ["data/short-data.csv"]


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


def merge_and_write_profiles(profiles: List[str], file_name: str):
    profiles = list(
        map(DatasetProfile.from_protobuf_string,  profiles))

    profile = reduce(lambda acc, cur: acc.merge(cur),
                     profiles, DatasetProfile(""))

    profile.write_protobuf(file_name)


if __name__ == "__main__":
    ray.init()
    # responses = [normal_hi() for i in range(10)]
    # print(responses )

    # responses = [hi.remote() for i in range(10)]
    # rayRespones = ray.get(responses)

    # This is sweet
    # actors = [MyActor.remote() for i in range(10)]
    # results = list(map(lambda actor: actor.inc.remote(), actors))
    # print(ray.get(results))

    print("========== Serial =============")
    serial_start = time.time()
    merge_and_write_profiles(run_serial(), "serial.bin")
    print(f"Serial time {time.time() - serial_start}")

    print()
    print("========== Parallel =============")
    serial_start = time.time()
    merge_and_write_profiles(run_parallel(), "parallel.bin")
    print(f"Parallel time {time.time() - serial_start}")
