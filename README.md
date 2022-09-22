This is an example repo that shows some ways of integrating whylogs into ray. Ray is pretty flexible and whylogs is essentially a function that turns data into profiles so there are a lot of different ways to organize things.

You can reach out to our [slack](https://whylabs.ai/slack-community) for questions or feedback.

# Setup

```python
# Using poetry
poetry install
poetry shell

# Using virtualenv
pip install -r requirements.txt
source ./env/bin/activate

# Then run the examples.
python main-pipelines.py
python main-serve-pipeline.py
python main-functions.py
```

There are two kinds of files in here: `main-*` files and `main-serve-*` files. The former just run some self contained example ray/whylogs code while the later use ray serve. For the serve ones, you can send data with curl once they're up and running.

```bash
# To get the current state of the whylogs profile
curl 'http://127.0.0.1:8000/Logger'

# To upload the packaged csv data files.
curl  'http://127.0.0.1:8000/MyModel' -H 'Content-Type: text/plain' --data-binary @data/data1.csv
```
