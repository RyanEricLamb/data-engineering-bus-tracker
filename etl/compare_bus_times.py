from datetime import datetime
import pandas as pd
import pytz
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from pathlib import Path
import os


tz = pytz.timezone("UTC")
now = datetime.now(tz)
dt = now.replace(hour=0, minute=0, second=0, microsecond=0)


@task(log_prints=True, retries=3)
def get_timetable_from_gcs(
    current_timetable_filename: str, pref_gcs_block_name: str
) -> Path:
    """Retrieve current timetable from bucket"""

    gcs_path = f"current_timetable/{current_timetable_filename}.parquet.gzip"
    gcs_block = GcsBucket.load(pref_gcs_block_name)
    # Download timetable to cwd
    gcs_block.get_directory(from_path=gcs_path)

    return Path(gcs_path)


@task(log_prints=True, retries=3)
def get_live_locations_from_gcs(
    live_locations_filename: str, pref_gcs_block_name: str
) -> Path:
    """Retrieve live locations from bucket"""

    gcs_path = f"live_location/{live_locations_filename}.parquet.gzip"
    gcs_block = GcsBucket.load(pref_gcs_block_name)
    # Download live locations to cwd
    gcs_block.get_directory(from_path=gcs_path)

    return Path(gcs_path)


@task()
def combine_live_trips_with_timetable(
    trips_today: pd.DataFrame, live_locations: pd.DataFrame
) -> pd.DataFrame:
    """Merge all scheduled timetable trips with live trip data"""

    compare = trips_today.merge(
        live_locations,
        left_on=["trip_id", "stop_sequence"],
        right_on=["trip_id", "current_stop"],
    )

    return compare


@task()
def calculate_late_buses(compare: pd.DataFrame) -> pd.DataFrame:
    """Calculate difference between bus scheduled time and actual live time"""

    # Resolve times that flow over to next day (e.g. 26:00 hours)
    compare.loc[:, "arrival_time_fixed"] = dt + pd.to_timedelta(compare["arrival_time"])
    compare.loc[:, "departure_time_fixed"] = dt + pd.to_timedelta(
        compare["departure_time"]
    )

    # Compare current time at stop with expected arrival time
    compare["arrival_time_fixed"] = pd.to_datetime(compare["arrival_time_fixed"])
    compare["time_diff"] = (
        compare["timestamp"] - compare["arrival_time_fixed"]
    ) / pd.Timedelta(minutes=1)

    # Remove timestamps that are not within the last 30mins
    late_buses = compare[(now - compare["timestamp"]) / pd.Timedelta(minutes=1) <= 30]

    # Get buses later than 10 minutes at specific stop and remove current_status == 1
    late_buses = late_buses[
        (late_buses["time_diff"] > 10) & (late_buses["current_status"] != 1)
    ]

    return late_buses


@task()
def load_late_buses_to_gcs(late_buses_path: Path, pref_gcs_block_name: str) -> None:
    """Upload late buses to GCS"""

    gcs_block = GcsBucket.load(pref_gcs_block_name)
    gcs_block.upload_from_path(from_path=late_buses_path)

    return None


@flow()
def compare_bus_times(
    current_timetable_filename: str = "timetable_today",
    live_locations_filename: str = "live_location",
    pref_gcs_block_name: str = "bus-tracker-gcs-bucket",
):

    trips_today_path = get_timetable_from_gcs(
        current_timetable_filename, pref_gcs_block_name
    )
    trips_today = pd.read_parquet(trips_today_path)

    live_locations_path = get_live_locations_from_gcs(
        live_locations_filename, pref_gcs_block_name
    )
    live_locations = pd.read_parquet(live_locations_path)

    compare = combine_live_trips_with_timetable(
        wait_for=[trips_today, live_locations],
        trips_today=trips_today,
        live_locations=live_locations,
    )

    os.remove(trips_today_path)
    os.remove(live_locations_path)

    late_buses = calculate_late_buses(wait_for=[compare], compare=compare)
    late_buses.to_csv("late_buses.csv", index=False)

    load_late_buses_to_gcs(
        wait_for=[late_buses],
        late_buses_path="late_buses.csv",
        pref_gcs_block_name=pref_gcs_block_name,
    )

    os.remove("late_buses.csv")


if __name__ == "__main__":

    compare_bus_times()
