from datetime import datetime
import gtfs_kit as gk
import numpy as np
import pandas as pd
import pytz
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
import requests
import os


@task()
def timetables_feed(timetable_url: str) -> gk.feed:
    """Get latest timetable GTFS file from Open Bus Data service"""

    r = requests.get(timetable_url)
    with open("gtfs_timetables.zip", "wb") as fd:
        for chunk in r.iter_content(chunk_size=128):
            fd.write(chunk)

    feed = gk.read_feed("gtfs_timetables.zip", dist_units="mi")

    os.remove("gtfs_timetables.zip")

    return feed


@task()
def add_stops_timetable(feed: gk.feed, agency_name: str) -> pd.DataFrame:
    """Add stops and stop times to each trip for the selected operator"""

    # Get operator id
    agency_id = feed.agency["agency_id"][
        feed.agency["agency_name"] == agency_name
    ].values[0]

    # Find associated routes
    routes = feed.routes[feed.routes.agency_id == agency_id]

    # Join routes to trips
    trips = feed.trips

    trips_routes = trips.merge(routes, how="left", on="route_id")

    # Remove trips that aren't part of the selected operator
    trips_routes["agency_id"].replace("", np.nan, inplace=True)
    trips_routes.dropna(subset=["agency_id"], inplace=True)

    # Add calendar_dates of service
    cal_dates = feed.calendar
    trips_dates = trips_routes.merge(cal_dates, how="left", on="service_id")

    # Add stop times
    stop_times = feed.stop_times
    trips_stops = trips_dates.merge(stop_times, how="left", on="trip_id")

    # Add stops
    stops = feed.stops
    trips_stops = trips_stops.merge(stops, how="left", on="stop_id")

    return trips_stops


@task()
def timetable_today(
    trips_stops: pd.DataFrame, current_trips_filename: str
) -> pd.DataFrame:
    """Transform all trip timetables to include only those running on the current (UK UTC) day"""

    tz = pytz.timezone("UTC")
    date_uk = datetime.now(tz)

    current_day_uk = date_uk.strftime("%A").lower()

    date_uk_str = date_uk.strftime("%Y%m%d")
    date_uk = pd.to_datetime(date_uk_str, format="%Y%m%d")

    # Convert start & end dates to datetime
    trips_stops[["start_date", "end_date"]] = trips_stops[
        ["start_date", "end_date"]
    ].apply(pd.to_datetime, format="%Y%m%d")

    # Only use trips if current day is in the service interval
    trips_stops = trips_stops[
        (date_uk >= trips_stops["start_date"]) & (date_uk <= trips_stops["end_date"])
    ]

    # If in service interval, check if trip is valid on the same day of week
    trips_today = trips_stops[trips_stops[current_day_uk] == 1]

    trips_today.to_parquet(f"{current_trips_filename}.parquet.gzip", compression="gzip")

    return trips_today


@task()
def load_timetable_to_gcs(
    pref_gcs_block_name: str, from_path: str, to_path: str
) -> None:
    """Load the trips today timetable to Google Bucket"""

    gcs_block = GcsBucket.load(pref_gcs_block_name)
    gcs_block.upload_from_path(from_path=from_path, to_path=to_path)

    os.remove(from_path)

    return None


@flow()
def get_bus_timetables(
    timetable_url: str = "https://data.bus-data.dft.gov.uk/timetable/download/gtfs-file/yorkshire/",
    agency_name: str = "First Leeds",
    current_timetable_filename: str = "timetable_today",
    pref_gcs_block_name: str = "bus-tracker-gcs-bucket",
) -> None:

    full_timetable = timetables_feed(timetable_url)

    trips_stops = add_stops_timetable(
        wait_for=[full_timetable], feed=full_timetable, agency_name=agency_name
    )
    trips_today = timetable_today(
        wait_for=[trips_stops],
        trips_stops=trips_stops,
        current_trips_filename=current_timetable_filename,
    )
    load_timetable_to_gcs(
        wait_for=[trips_today],
        pref_gcs_block_name=pref_gcs_block_name,
        from_path=f"{current_timetable_filename}.parquet.gzip",
        to_path=f"current_timetable/{current_timetable_filename}.parquet.gzip",
    )
    return None


if __name__ == "__main__":

    get_bus_timetables()
