import requests
import pandas as pd
from io import BytesIO
from prefect import task, flow
from prefect_gcp.cloud_storage import GcsBucket


@task(log_prints=True, name="GET FROM WEB")
def get_from_web(dataset_url: str) -> bytes:
    raw = requests.get(dataset_url)
    return raw.content


@task(log_prints=True, name="TO PARQUET")
def df_to_parquet(raw: bytes) -> pd.DataFrame:
    df = pd.read_csv(BytesIO(raw), compression="gzip")
    buffer = BytesIO()
    df.to_parquet(buffer, engine="auto", compression="snappy")

    print(f"amount of rows : {len(df)}")

    buffer.seek(0)
    return buffer


@task(log_prints=True, name="TO BUCKET")
def upload_to_bucket(encoded: bytes, storage_url: str) -> None:
    gcs_block = GcsBucket.load("dtc-gcs-bucket")
    gcs_block.upload_from_file_object(encoded, storage_url)


@flow(log_prints=True)
def data_to_bucket(color: str, year: int, month: int) -> None:
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{color}_tripdata_{year}-{month:02}.csv.gz"
    storage_url = f"{color}_taxi_data/{color}_tripdata_{year}-{month:02}.parquet"

    raw = get_from_web(dataset_url)
    parquet = df_to_parquet(raw)
    upload_to_bucket(parquet, storage_url)


if __name__ == "__main__":
    color = "green"
    year = 2020
    month = 11
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{color}_tripdata_{year}-{month:02}.csv.gz"
    storage_url = f"{color}_taxi_data/{color}_tripdata_{year}-{month:02}.parquet"

    raw = get_from_web(dataset_url)
    parquet = df_to_parquet(raw)
    upload_to_bucket(parquet, storage_url)
