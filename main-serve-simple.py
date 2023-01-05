import asyncio
import io
import time

import pandas as pd
import ray
from ray import serve
from starlette.requests import Request
from whylogs.core import DatasetProfile, DatasetProfileView

ray.init()
serve.start()

batch_size = 1000


@ray.remote
def log_frame(df: pd.DataFrame) -> DatasetProfileView:
    profile = DatasetProfile()
    profile.track(df)
    return profile.view()


@serve.deployment()
class Logger:
    def __init__(self) -> None:
        # TODO having to make views like this is weird when the DatasetProfileView constructor exists
        self.profile = DatasetProfile().view()
        self.profile_queue = asyncio.Queue()
        asyncio.create_task(self.read_profiles())

    async def read_profiles(self):
        while True:
            profile = await self.profile_queue.get()
            self.profile = self.profile.merge(profile)

    def log(self, df: pd.DataFrame):
        profile = ray.get(log_frame.remote(df))
        # TODO is it better to just send the dataframes here via the queue? What costs more, tracking or merging?
        asyncio.create_task(self.profile_queue.put(profile))

    async def __call__(self, request: Request):
        try:
            return str(self.profile.to_pandas())
        except:
            return []


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
