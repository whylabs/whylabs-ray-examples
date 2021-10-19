import asyncio
import io
import time
from functools import reduce
from typing import List

import pandas as pd
import ray
from ray import serve
from ray.data.dataset_pipeline import DatasetPipeline
from starlette.requests import Request
from whylogs.app import Session
from whylogs.app.writers import WhyLabsWriter
from whylogs.core.datasetprofile import DatasetProfile

ray.init()
serve.start()

batch_size = 1000


@ray.remote
def log_frame(df: pd.DataFrame) -> List[bytes]:
    session = Session(
        project="default-project",
        pipeline="default-pipeline",
        writers=[])
    logger = session.logger("")
    logger.log_dataframe(df)
    return logger.profile.serialize_delimited()


def merge_profiles(profiles: List[bytes]) -> DatasetProfile:
    """
    Utility function that has we use to merge our many generated profiles into a single one.
    """
    profiles = map(
        lambda profile: DatasetProfile.parse_delimited_single(profile)[1],
        profiles)
    profile = reduce(
        lambda acc, cur: acc.merge(cur),
        profiles,
        DatasetProfile(""))
    return profile


# TODO Is global singleton state like this an antipattern? This actually fails at runtime with a pickle error, but is something like this possible?
profile = DatasetProfile("")
profile_queue = asyncio.Queue()


async def read_profiles():
    while True:
        profile = profile.merge(await profile_queue.get())
task = read_profiles()


@serve.deployment()
class Logger:
    def __init__(self) -> None:
        pass

    def _log_remote(self, df: pd.DataFrame) -> List[bytes]:
        serialized_profile = log_frame.remote(df)
        results = [ray.get(serialized_profile)]
        return results

    def log(self, df: pd.DataFrame):
        results = self._log_remote(df)
        # TODO this line actually causes the pickling failure as long as profile_queue is referenced. Replacing self.profile_queue has no issues so I assume it's related to state not being local.
        asyncio.create_task(profile_queue.put(merge_profiles(results)))

    async def __call__(self, request: Request):
        return str(profile.to_summary())


@serve.deployment
class MyModel:
    def __init__(self) -> None:
        self.logger = Logger.get_handle(sync=True)

    def predict(self, df: pd.DataFrame):
        # implement with a real model
        return []

    async def __call__(self, request: Request):
        bytes = await request.body()
        csv_text = bytes.decode(encoding='UTF-8')
        df = pd. read_csv(io.StringIO(csv_text))
        # log the data with whylogs asynchronously using the dedicated logging endpoint.
        self.logger.log.remote(df)
        return self.predict(df)


Logger.deploy()
MyModel.deploy()

while True:
    time.sleep(5)
