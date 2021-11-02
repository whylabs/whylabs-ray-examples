import asyncio
import io
import time
from functools import reduce
from typing import List

import pandas as pd
import ray
from ray import serve
from starlette.requests import Request
from whylogs.core.datasetprofile import DatasetProfile

ray.init()
serve.start()

batch_size = 1000


@ray.remote
def log_frame(df: pd.DataFrame) -> DatasetProfile:
    profile = DatasetProfile("")
    profile.track_dataframe(df)
    return profile


@serve.deployment()
class Logger:
    def __init__(self) -> None:
        # TODO how long will this local state stick around?
        # TODO How many instances of Logger are actually "deployed"?
        # TODO Can I pull state for all instances of Logger at some point to retrieve these profiles or do I need to coordinate that with some supervisor actor?
        self.profile = DatasetProfile("")
        self.profile_queue = asyncio.Queue()
        asyncio.create_task(self.read_profiles())

    # TODO is this required? This is the only thing ensuring that `self.profile` isn't modififed concurrently. Should I treat these deployments as "controllers" in typical server frameworks, requiring thread local objects?
    async def read_profiles(self):
        while True:
            profile = await self.profile_queue.get()
            self.profile = self.profile.merge(profile)

    def log(self, df: pd.DataFrame):
        profile = ray.get(log_frame.remote(df))
        asyncio.create_task(self.profile_queue.put(profile))

    async def __call__(self, request: Request):
        return str(self.profile.to_summary())


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
