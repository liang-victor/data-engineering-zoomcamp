from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials
from typing import List


@task(retries=3)
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    """Download trip data from GCS"""
    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.get_directory(from_path=gcs_path, local_path=f"../data/")
    return Path(f"../data/{gcs_path}")


@task()
def transform(path: Path) -> pd.DataFrame:
    """Data cleaning example"""
    df = pd.read_parquet(path)
    
    return df


@task()
def write_bq(df: pd.DataFrame) -> None:
    """Write DataFrame to BiqQuery"""

    gcp_credentials_block = GcpCredentials.load("gcp-victor")

    df.to_gbq(
        destination_table="dezoomcamp.rides",
        project_id="dtc-de-course-375316",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append",
    )


@flow()
def etl_gcs_to_bq(month, year, color):
    """Main ETL flow to load data into Big Query"""
    path = extract_from_gcs(color, year, month)
    df = transform(path)
    row_count = len(df)

    write_bq(df)
    return row_count

@flow()
def etl_gcs_to_bq_parent_flow(months: List[int], year: int, color: str):
    total_rows = 0
    for month in months:
        rows = etl_gcs_to_bq(month= month, year = year, color = color)
        total_rows += rows
    print(f"Total rows processed { total_rows }")


if __name__ == "__main__":
    etl_gcs_to_bq()
