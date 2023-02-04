from pathlib import Path  # py-standard lib usefull for dealing with file paths
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket  # to push data to GCS bucket
from random import randint


@task(retries=3)
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read taxi data from web into pandas DataFrame"""
    # if randint(0, 1) > 0:
    #     raise Exception

    df = pd.read_csv(dataset_url)  # transforms csv to panda dataframe
    return df


@task(log_prints=True)
def clean(df: pd.DataFrame, color: str) -> pd.DataFrame:
    """Fix dtype issues"""

    # transforms string column to a datetime column
    if color == "yellow":
        df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
        df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])
    else:
        df["lpep_pickup_datetime"] = pd.to_datetime(df["lpep_pickup_datetime"])
        df["lpep_dropoff_datetime"] = pd.to_datetime(df["lpep_dropoff_datetime"])

    print(df.head(2))
    print(f"columns: {df.dtypes}")  # print column types
    print(f"rows: {len(df)}")  # df = complete grid, hence len(df) = # rows
    return df


@task(log_prints=True)
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    """Write DataFrame out locally as parquet file"""

    # path = Path(__file__).parent  # /home/vincenzo/prefect-zoomcamp/flows/02_gcp
    local_path = Path(f"/flows/02_gcp/{color}_taxi_data/{dataset_file}.parquet")
    gc_path = Path(f"./{color}_taxi_data/{dataset_file}.parquet")

    df.to_parquet(local_path, compression="gzip")
    return [local_path, gc_path]


@task()
def write_gcs(local_path: Path, gc_path: Path) -> None:
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load("dtc-gcs-bucket")
    gcs_block.upload_from_path(from_path=local_path, to_path=gc_path)
    return


@flow()
def etl_web_to_gcs(color: str = "green", year: int = 2020, months: list = [11]) -> None:
    """The main ETL function for web to bucket"""

    counter = 0

    for month in months:
        dataset_file = f"{color}_tripdata_{year}-{month:02}"
        dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

        df = fetch(dataset_url)  # put taxi data into a pandas data frame
        df_clean = clean(df, color)  # transform data
        counter = +len(df)
        path = write_local(df_clean, color, dataset_file)
        local_path = path[0]
        gc_path = path[1]
        write_gcs(local_path, gc_path)

    print(f"Total processed rows: {counter}")


if __name__ == "__main__":
    color = "green"
    year = 2020
    months = [11]
    etl_web_to_gcs(color, year, months)
