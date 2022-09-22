import asyncio
import io
import time
from functools import reduce
from typing import List

import pandas as pd
import ray
from ray import serve
from starlette.requests import Request
from whylogs.core import DatasetProfile, DatasetProfileView

ray.init()
serve.start()


@ray.remote
def log_frame(df: pd.DataFrame) -> DatasetProfileView:
    profile = DatasetProfile()
    profile.track(df)
    return profile


# TODO Is global singleton state like this an antipattern? This actually fails at runtime with a pickle error, but is something like this possible?
profile = DatasetProfile().view
profile_queue = asyncio.Queue()


async def read_profiles():
    while True:
        profile = profile.merge(await profile_queue.get())


@serve.deployment()
class Logger:
    def log(self, df: pd.DataFrame):
        profile = ray.get(log_frame.remote(df))
        # TODO this line actually causes the pickling failure as long as profile_queue is referenced. Replacing self.profile_queue has no issues so I assume it's related to state not being local.
        asyncio.create_task(profile_queue.put(profile))

    async def __call__(self, request: Request):
        return str(profile.to_pandas())


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
        df = pd.read_csv(io.StringIO(csv_text))
        # log the data with whylogs asynchronously using the dedicated logging endpoint.
        self.logger.log.remote(df)
        return self.predict(df)


Logger.deploy()
MyModel.deploy()

asyncio.create_task(read_profiles())
while True:
    time.sleep(5)
