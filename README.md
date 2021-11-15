

# Running

```
# Setup dependencies with
poetry install
poetry run python main.py

# Or

pip install -r requirements.txt
python main.py

# Then run the examples.
poetry shell # if using poetry
python main-pipelines.py
python main-serve-pipeline.py
python main-functions.py
```

It has a few examples that show how whylogs might be integrated into a ray setup
in general. One of the example setups doesn't work with WSL but it does work on
Linux. Haven't tested these on Mac yet.

It runs each example and spits out how long it took at the end.
