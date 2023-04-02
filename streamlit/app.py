import streamlit as st
from google.oauth2 import service_account
from google.cloud import storage
import folium
from streamlit_folium import st_folium
from datetime import datetime, timezone
import pandas as pd
from io import StringIO


def refresh_map():
    """Refreshes all app data"""

    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    # Create API client for gcs bucket
    credentials = service_account.Credentials.from_service_account_info(
        st.secrets["gcp_service_account"]
    )
    client = storage.Client(credentials=credentials)

    m = folium.Map(location=[53.799, -1.549], tiles="Stamen Terrain", zoom_start=11.5)

    def read_file(bucket_name, file_path):
        """Get bucket content"""

        bucket = client.bucket(bucket_name)
        content = bucket.blob(file_path).download_as_string().decode("utf-8")
        return content

    def get_df_from_bucket():
        """Reads the bucket csv and converts to dataframe"""

        bucket_name = "bus-tracking-376121-bus_data"
        file_path = "late_buses.csv"

        data = read_file(bucket_name, file_path)
        df = pd.read_csv(StringIO(data))

        return df

    def create_marker(
        lat, long, route, stop_name, vehicle, scheduled_time, actual_time
    ):
        """Add a circle marker to the map"""

        marker = folium.Circle(
            location=[lat, long],
            tooltip=f"<b>Route: {route} </b><br>Stop: {stop_name} <br>Vehicle: {vehicle} <br>Scheduled : {scheduled_time} <br>Actual : {actual_time}",
            radius=100,
            color="crimson",
            fill=True,
        ).add_to(m)

        return marker

    try:
        # Uncomment this when going live to read csv file from GCP
        # df = get_df_from_bucket()
        # Remove this when going live
        df = pd.read_csv("late_buses.csv")

        number_of_late_buses = df["trip_id"].count()

        for idx, row in df.iterrows():

            lat = row["stop_lat"]
            long = row["stop_lon"]
            stop = row["stop_name"]
            route = row["route_short_name"] + " - " + row["trip_headsign"]
            vehicle = row["vehicle"]
            scheduled_time = row["arrival_time_fixed"].split("+", 1)[0]
            actual_time = row["timestamp"].split("+", 1)[0]

            try:
                marker = create_marker(
                    lat, long, route, stop, vehicle, scheduled_time, actual_time
                )
                marker.add_to(m)
            except:
                print("adding marker failed")
    except Exception as e:
        print(e)
        number_of_late_buses = 0

    st.title("First Bus Leeds delays🚍")
    st.subheader(
        f"Number of buses currently more than 10 minutes late: {number_of_late_buses}",
    )
    st.subheader(
        f"IMPORTANT: This demo app is no longer live. Live functionality can be recreated - see the [project walkthrough](https://medium.com/@ryanelamb/a-data-engineering-project-with-prefect-docker-terraform-google-cloudrun-bigquery-and-streamlit-3fc6e08b9398?source=friends_link&sk=c83c07681d2af63d8292c2bac9e4287a)"
    )
    st.write(
        f"[Project GitHub](https://github.com/RyanEricLamb/data-engineering-bus-tracker) | [Project walkthrough](https://medium.com/@ryanelamb/a-data-engineering-project-with-prefect-docker-terraform-google-cloudrun-bigquery-and-streamlit-3fc6e08b9398?source=friends_link&sk=c83c07681d2af63d8292c2bac9e4287a) | [Source data](https://data.bus-data.dft.gov.uk/) "
    )
    # st.text(f"Click refresh below to update")

    st_data = st_folium(m, width=725, returned_objects=[])

    st.text("Updates every 2 minutes")
    st.text(f"Last refreshed: {now} UTC")


st.button("Refresh", on_click=refresh_map())
