from datetime import datetime
import pandas as pd
import pytz
import os
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
import requests
from google.transit.gtfs_realtime_pb2 import FeedMessage
from http import HTTPStatus
from prefect.blocks.system import Secret


# For local env variable instead of Prefect Cloud
# env_path = Path(".") / ".env"
# load_dotenv(dotenv_path=env_path)
# bods_api_key = os.environ["BODS_API"]

secret_block = Secret.load("bods-api-key")
bods_api_key = secret_block.get()


@task(log_prints=True, retries=3)
def get_live_gtfs(
    min_lat: float, max_lat: float, min_long: float, max_long: float, filename: str
) -> None:

    """Get live bus locations from Open Bus Data GTFS feed for area specified by bounding box coordinates"""

    url = f"https://data.bus-data.dft.gov.uk/api/v1/gtfsrtdatafeed/?boundingBox={min_lat},{max_lat},{min_long},{max_long}&api_key={bods_api_key}"

    response = requests.get(url, bods_api_key)

    if response.status_code == HTTPStatus.OK:
        message = FeedMessage()
        message.ParseFromString(response.content)

    trips = []
    for t in message.entity:
        trips.append(t)

    rows = [
        {
            "id": t.id,
            "trip_id": t.vehicle.trip.trip_id,
            "route_id": t.vehicle.trip.route_id,
            "start_time": t.vehicle.trip.start_time,
            "start_date": t.vehicle.trip.start_date,
            "latitude": t.vehicle.position.latitude,
            "longitude": t.vehicle.position.longitude,
            "current_stop": t.vehicle.current_stop_sequence,
            "current_status": t.vehicle.current_status,
            "timestamp": datetime.utcfromtimestamp(t.vehicle.timestamp),
            "vehicle": t.vehicle.vehicle.id,
        }
        for t in trips
    ]
    df = pd.DataFrame(rows).drop_duplicates()

    # Timestamps are published in UTC
    df["timestamp"] = df["timestamp"].dt.tz_localize("UTC")

    # Remove data not published on current date
    tz = pytz.timezone("UTC")
    today = datetime.now(tz).date()

    df["timestamp"] = pd.to_datetime(df["timestamp"], format="%Y-%m-%d %H:%M:%S.%f")

    df = df[df["timestamp"].dt.date == today]

    # Rename fields
    df.rename(
        {"start_date": "start_date_live", "route_id": "route_id_live"},
        axis=1,
        inplace=True,
    )

    df.to_parquet(f"{filename}.parquet.gzip", compression="gzip")

    return None


@task()
def load_live_locations_to_gcs(
    pref_gcs_block_name: str, from_path: str, to_path: str
) -> None:
    """Load the live bus locations to Google Bucket"""

    gcs_block = GcsBucket.load(pref_gcs_block_name)
    gcs_block.upload_from_path(from_path=from_path, to_path=to_path)

    os.remove(from_path)
    return None


@flow()
def get_live_bus_locations(
    area_coords: dict = {
        "min_lat": 53.725,
        "max_lat": 53.938,
        "min_long": -1.712,
        "max_long": -1.296,
    },
    pref_gcs_block_name: str = "bus-tracker-gcs-bucket",
    live_locations_filename: str = "live_location",
):

    get_live_gtfs(
        area_coords.get("min_lat"),
        area_coords.get("max_lat"),
        area_coords.get("min_long"),
        area_coords.get("max_long"),
        filename=live_locations_filename,
    )

    load_live_locations_to_gcs(
        wait_for=[get_live_gtfs],
        pref_gcs_block_name=pref_gcs_block_name,
        from_path=f"{live_locations_filename}.parquet.gzip",
        to_path=f"live_location/{live_locations_filename}.parquet.gzip",
    )


if __name__ == "__main__":

    get_live_bus_locations()
