import re
from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from kaggle.api.kaggle_api_extended import KaggleApi


@task(retries=3)
def fetch(path) -> pd.DataFrame:
    """Read taxi data from web into pandas DataFrame"""
    api = KaggleApi()
    api.authenticate()
    df1 = pd.read_csv(f"{path}{'2023_rankings.csv'}")
    df2 = pd.read_csv(f"{path}{'2022_rankings.csv'}")
    df3 = pd.read_csv(f"{path}{'2021_rankings.csv'}")
    df4 = pd.read_csv(f"{path}{'2020_rankings.csv'}")
    df5 = pd.read_csv(f"{path}{'2019_rankings.csv'}")
    df1['year'] = 2023
    df2['year'] = 2022
    df3['year'] = 2021
    df4['year'] = 2020
    df5['year'] = 2019
    combined_df = pd.concat([df1, df2, df3, df4, df5])
    combined_df['rank'] = combined_df['rank'].apply(lambda x: "".join([re.sub('^=', '', s) for s in x]))
    combined_df['year'] = combined_df['year'].astype('int64')
    return combined_df


@task()
def write_local(df: pd.DataFrame, service: str, dataset_file: str) -> Path:
    """Write DataFrame out locally as parquet file"""
    path = Path(f"{service}/{dataset_file}")
    df.to_parquet(path, compression="gzip")
    return path


@task()
def write_gcs(path: Path) -> None:
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load("de-zoom")
    gcs_block.upload_from_path(from_path=path, to_path=path)
    return


@flow()
def etl_web_to_gcs() -> None:
    """The main ETL function"""
    path = '/data-collisions/workflows/web_to_gcs/the-world-university-rankings-2011-2023/'
    service = "data"
    dataset_file = '2019_2023_rankings.parquet'
    df = fetch(path)
    path = write_local(df, service, dataset_file)
    write_gcs(path)


if __name__ == "__main__":
    etl_web_to_gcs()
