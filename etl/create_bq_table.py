from prefect_gcp.bigquery import bigquery_create_table
from google.cloud.bigquery import SchemaField
from prefect_gcp import GcpCredentials
from prefect import flow


@flow
def create_biqquery_table():
    gcp_project_id = "bus-tracking-376121"
    gcp_credentials = GcpCredentials(project=gcp_project_id)

    schema = [
        SchemaField("route_id", field_type="INTEGER", mode="REQUIRED"),
        SchemaField("service_id", field_type="INTEGER", mode="REQUIRED"),
        SchemaField("trip_id", field_type="STRING", mode="REQUIRED"),
        SchemaField("trip_headsign", field_type="STRING", mode="REQUIRED"),
        SchemaField("block_id", field_type="STRING", mode="REQUIRED"),
        SchemaField("shape_id", field_type="STRING", mode="REQUIRED"),
        SchemaField("wheelchair_accessible", field_type="INTEGER", mode="REQUIRED"),
        SchemaField("vehicle_journey_code", field_type="STRING", mode="REQUIRED"),
        SchemaField("agency_id", field_type="STRING", mode="REQUIRED"),
        SchemaField("route_short_name", field_type="STRING", mode="REQUIRED"),
        SchemaField("route_long_name", field_type="STRING", mode="NULLABLE"),
        SchemaField("route_type", field_type="STRING", mode="NULLABLE"),
        SchemaField("monday", field_type="INTEGER", mode="REQUIRED"),
        SchemaField("tuesday", field_type="INTEGER", mode="REQUIRED"),
        SchemaField("wednesday", field_type="INTEGER", mode="REQUIRED"),
        SchemaField("thursday", field_type="INTEGER", mode="REQUIRED"),
        SchemaField("friday", field_type="INTEGER", mode="REQUIRED"),
        SchemaField("saturday", field_type="INTEGER", mode="REQUIRED"),
        SchemaField("sunday", field_type="INTEGER", mode="REQUIRED"),
        SchemaField("start_date", field_type="DATE", mode="REQUIRED"),
        SchemaField("end_date", field_type="DATE", mode="REQUIRED"),
        SchemaField("arrival_time", field_type="TIME", mode="REQUIRED"),
        SchemaField("departure_time", field_type="TIME", mode="REQUIRED"),
        SchemaField("stop_id", field_type="INTEGER", mode="REQUIRED"),
        SchemaField("stop_sequence", field_type="INTEGER", mode="REQUIRED"),
        SchemaField("stop_headsign", field_type="STRING", mode="NULLABLE"),
        SchemaField("pickup_type", field_type="INTEGER", mode="NULLABLE"),
        SchemaField("drop_off_type", field_type="INTEGER", mode="NULLABLE"),
        SchemaField("shape_dist_traveled", field_type="FLOAT64", mode="NULLABLE"),
        SchemaField("timepoint", field_type="INTEGER", mode="NULLABLE"),
        SchemaField("stop_code", field_type="INTEGER", mode="NULLABLE"),
        SchemaField("stop_name", field_type="STRING", mode="REQUIRED"),
        SchemaField("stop_lat", field_type="FLOAT64", mode="REQUIRED"),
        SchemaField("stop_long", field_type="FLOAT64", mode="REQUIRED"),
        SchemaField("wheelchair_boarding", field_type="NUMERIC", mode="NULLABLE"),
        SchemaField("location_type", field_type="STRING", mode="NULLABLE"),
        SchemaField("parent_station", field_type="STRING", mode="NULLABLE"),
        SchemaField("platform_code", field_type="STRING", mode="NULLABLE"),
        SchemaField("id", field_type="STRING", mode="REQUIRED"),
        SchemaField("route_id_live", field_type="INTEGER", mode="REQUIRED"),
        SchemaField("start_time", field_type="TIME", mode="REQUIRED"),
        SchemaField("start_date_live", field_type="STRING", mode="REQUIRED"),
        SchemaField("latitude", field_type="FLOAT64", mode="REQUIRED"),
        SchemaField("longitude", field_type="FLOAT64", mode="REQUIRED"),
        SchemaField("current_stop", field_type="INTEGER", mode="REQUIRED"),
        SchemaField("current_status", field_type="INTEGER", mode="NULLABLE"),
        SchemaField("timestamp", field_type="STRING", mode="REQUIRED"),
        SchemaField("vehicle", field_type="STRING", mode="REQUIRED"),
        SchemaField("arrival_time_fixed", field_type="STRING", mode="REQUIRED"),
        SchemaField("departure_time_fixed", field_type="STRING", mode="REQUIRED"),
        SchemaField("time_diff", field_type="FLOAT64", mode="REQUIRED"),
    ]

    bigquery_create_table(
        dataset="bus_tracker",
        table="raw_late_buses",
        schema=schema,
        gcp_credentials=gcp_credentials,
    )


if __name__ == "__main__":

    create_biqquery_table()
