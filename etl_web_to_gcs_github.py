import requests
import pandas as pd
from io import BytesIO
from prefect import task, flow
from prefect_gcp.cloud_storage import GcsBucket


def get_from_web(dataset_url: str) -> bytes:
    raw = requests.get(dataset_url)
    return raw.content


def df_to_parquet(raw: bytes) -> pd.DataFrame:
    df = pd.read_csv(BytesIO(raw), compression="gzip")
    buffer = BytesIO()
    df.to_parquet(buffer, engine="auto", compression="snappy")

    print(f"amount of rows : {len(df)}")

    buffer.seek(0)
    return buffer


# 2 save as raw
def upload_to_bucket(encoded: bytes, storage_url: str) -> None:
    gcs_block = GcsBucket.load("dtc-gcs-bucket")
    gcs_block.upload_from_file_object(encoded, storage_url)


# 3 save as paquet to gcbucket


if __name__ == "__main__":

    color = "green"
    year = 2020
    month = 11

    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{color}_tripdata_{year}-{month:02}.csv.gz"
    storage_url = f"{color}_taxi_data/{color}_tripdata_{year}-{month:02}.parquet"

    raw = get_from_web(dataset_url)
    parquet = df_to_parquet(raw)
    upload_to_bucket(parquet, storage_url)
