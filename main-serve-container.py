import json
import requests
import io
import time

import pandas as pd
import ray
from ray import serve
from starlette.requests import Request

ray.init()
serve.start()


@serve.deployment()
class Logger:
    def log(self, df: pd.DataFrame):
        # Post request with data as the payload to your whylogs container
        request = {
            'datasetId': '123',
            'tags': {},
            'multiple': df.to_dict(orient='split')
        }
        requests.post(
            'http://localhost:8080/logs',
            json.dumps(request),
            headers={'X-API-Key': 'password'})

    async def __call__(self, request: Request):
        return "NoOp"


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
