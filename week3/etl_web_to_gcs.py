from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket

@task(retries=3)
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read taxi data from web into pandas DataFrame"""
    df = pd.read_csv(dataset_url, low_memory=False, encoding='latin1')
    return df

@task
def write_local(df: pd.DataFrame, dataset_file: str) -> Path:
    """Write DataFrame localy as csv.gzip file"""
    Path(f"data/fhv/").mkdir(parents=True, exist_ok=True)
    path = Path(f"data/fhv/{dataset_file}.csv.gz")
    df.to_csv(path, compression="gzip", encoding='utf-8')
    return path

@task
def write_gcs(path: Path) -> None:
    """Upload local file to GCS"""
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.upload_from_path(from_path=f"{path}", to_path=path, timeout=3000)
    return

@flow()
def etl_web_to_gcs(year: int, months: list[int] = [1,2,3,4,5,6,7,8,9,10,11,12]) -> None:
    """The main ETL function"""
    
    for month in months:
        dataset_file = f"fhv_tripdata_{year}-{month:02}"
        dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/{dataset_file}.csv.gz"

        df = fetch(dataset_url)
        path = write_local(df, dataset_file)
        write_gcs(path)

if __name__ == "__main__":
    etl_web_to_gcs(2021, [1,2,3,4,5,6,7])