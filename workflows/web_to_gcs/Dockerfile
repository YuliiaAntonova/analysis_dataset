FROM prefecthq/prefect:2.7.7-python3.9

COPY requirements.txt .

RUN pip install -r requirements.txt --trusted-host pypi.python.org --no-cache-dir
WORKDIR /app

COPY etl_web_to_gcs.py etl_web_to_gcs.py