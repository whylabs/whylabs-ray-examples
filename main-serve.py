import io
from functools import reduce
import time
from threading import local
from typing import List

import mlflow
from ray.data.dataset_pipeline import DatasetPipeline
import pandas as pd
import ray
from ray import serve
from starlette.requests import Request
from whylogs.app import Session
from whylogs.app.writers import WhyLabsWriter
from whylogs.core.datasetprofile import DatasetProfile

ray.init()
serve.start()


model_uri = ''
batch_size = 1000


@ray.remote
def log_frame(df: pd.DataFrame) -> List[bytes]:
    session = Session(project="ignored", pipeline="ignored", writers=[])
    logger = session.logger("")
    logger.log_dataframe(df)
    return logger.profile.serialize_delimited()


def merge_and_write(profiles: List[bytes]) -> DatasetProfile:
    profiles = map(lambda profile: DatasetProfile.parse_delimited_single(
        profile)[1],  profiles)
    profile = reduce(lambda acc, cur: acc.merge(cur),
                     profiles, DatasetProfile(""))
    return profile


@serve.deployment(max_concurrent_queries=1)
class Logger:
    def __init__(self) -> None:
        self.profile = DatasetProfile("")

    def log(self, df: pd.DataFrame):
        pipeline = ray.data.from_pandas([df]).pipeline()
        pipelines = pipeline.iter_batches(
            batch_size=1000, batch_format="pandas")
        print("Logging each pandas frame")
        serialized_profiles = [log_frame.remote(batch) for batch in pipelines]
        print("Getting results")
        results = ray.get(serialized_profiles)
        print("Merging results")
        profile = merge_and_write(results)
        self.profile = self.profile.merge(profile)

    async def __call__(self, request: Request):
        return str(self.profile.to_summary())


@serve.deployment
class MyModel:
    def __init__(self) -> None:
        pass
        self.logger = Logger.get_handle(sync=False)
        # self.model = mlflow.pyfunc.load_model(model_uri=model_uri)

    # def tmp(self, df: pd.DataFrame):
    #     pipeline = ray.data.from_pandas([df]).pipeline()
    #     pipelines = pipeline.iter_batches(
    #         batch_size=1000, batch_format="pandas")
    #     print("Logging each pandas frame")
    #     serialized_profiles = [log_frame.remote(batch) for batch in pipelines]
    #     print("Getting results")
    #     results = ray.get(serialized_profiles)
    #     print("Merging results")
    #     profile = merge_and_write(results)
    #     print('orig')
    #     print(logger.profile.to_summary())
    #     new_profile = logger.profile.merge(profile)

    #     profile = DatasetProfile()

    #     print('new')
    #     print(new_profile.to_summary())
    #     return str(logger.profile.to_summary())

    async def __call__(self, request: Request):
        bytes = await request.body()
        csv_text = bytes.decode(encoding='UTF-8')
        df = pd. read_csv(io.StringIO(csv_text), sep=',')
        # self.model.predict(df)
        print("Sending the data frame over")
        await self.logger.log.remote(df)

        # return self.tmp(df)


Logger.deploy()
MyModel.deploy()

while True:
    time.sleep(5)
