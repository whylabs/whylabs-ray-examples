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
    profiles = map(
        lambda profile: DatasetProfile.parse_delimited_single(profile)[1],
        profiles)
    profile = reduce(
        lambda acc, cur: acc.merge(cur),
        profiles,
        DatasetProfile(""))
    return profile


@serve.deployment()
class Logger:
    def __init__(self) -> None:
        self.profile = DatasetProfile("")
        self.profile_queue = asyncio.Queue()
        asyncio.create_task(self.read_profiles())

    async def read_profiles(self):
        while True:
            profile = await self.profile_queue.get()
            await self.update_profile(profile)

    async def update_profile(self, profile: DatasetProfile):
        self.profile = self.profile.merge(profile)

    def _log_pipeline(self, df: pd.DataFrame) -> List[bytes]:
        pipeline = ray.data.from_pandas([df]).pipeline()
        pipelines = pipeline.iter_batches(
            batch_size=batch_size,
            batch_format="pandas")
        serialized_profiles = [log_frame.remote(batch) for batch in pipelines]
        results = ray.get(serialized_profiles)
        return results

    def log(self, df: pd.DataFrame):
        results = self._log_pipeline(df)
        profile = merge_profiles(results)
        asyncio.create_task(self.profile_queue.put(profile))

    async def __call__(self, request: Request):
        return str(self.profile.to_summary())


@serve.deployment
class MyModel:
    def __init__(self) -> None:
        self.logger = Logger.get_handle(sync=True)

    def predict(self, df: pd.DataFrame):
        # TODO implement with a real model
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
