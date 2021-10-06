import pandas as pd
import os
from whylogs.app import Session
from whylogs.app.writers import WhyLabsWriter

os.environ["WHYLABS_API_ENDPOINT"] = "https://songbird.development.whylabsdev.com"
os.environ["WHYLABS_API_KEY"] = "qqXHbAyuWl.YwwvmNDyOVZnqLJLjrhsoWDttDwvZUIPh5Le5J94WW0l1gPNMl8vQ"
os.environ["WHYLABS_DEFAULT_ORG_ID"] = "org-3543"

df = pd.read_csv("data.csv")

# Adding the WhyLabs Writer to utilize WhyLabs platform
writer = WhyLabsWriter("", formats=[])

session = Session(project="demo-project", pipeline="demo-pipeline", writers=[writer])
with session.logger(tags={"datasetId": "model-1"}) as ylog:
     ylog.log_dataframe(df)
