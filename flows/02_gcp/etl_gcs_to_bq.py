from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials


@task(log_prints=True)
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    """Download trip data from GCS"""

    gcs_path = f"./{color}_taxi_data/{color}_tripdata_{year}-{month:02}.parquet"

    gcs_block = GcsBucket.load("dtc-gcs-bucket")
    gcs_block.get_directory(from_path=gcs_path, local_path=gcs_path)

    return Path(f"{gcs_path}")


@task()
def transform(path: Path) -> pd.DataFrame:
    """Data cleaning example"""
    df = pd.read_parquet(path)
    print(f"pre: missing passenger count: {df['passenger_count'].isna().sum()}")
    # df["passenger_count"].fillna(0, inplace=True)
    print(f"post: missing passenger count: {df['passenger_count'].isna().sum()}")
    return df


@task()
def write_bq(df: pd.DataFrame, color: str) -> None:
    """Write DataFrame to BiqQuery"""

    gcp_credentials_block = GcpCredentials.load("dtc-gcp-credentials")

    df.to_gbq(
        destination_table=f"trips_data_all.{color}_taxi_data",
        project_id="dtc-data-engineering-375007",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append",
    )


@flow(log_prints=True)
def etl_gcs_to_bq(color: str, year: int, months: list):
    """Main ETL flow to load data into Big Query"""
    counter = 0

    for month in months:
        path = extract_from_gcs(color, year, month)
        df = transform(path)
        print(f"processing {len(df)} rows for this run")
        counter += len(df)
        write_bq(df, color)

    print(f"Totally processed {counter} rows")


if __name__ == "__main__":
    color = "yellow"
    year = 2019
    months = [2, 3]
    etl_gcs_to_bq(color, year, months)
