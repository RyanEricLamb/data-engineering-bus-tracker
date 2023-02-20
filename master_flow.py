from bus_live_locations import get_live_bus_locations
from compare_bus_times import compare_bus_times
from write_to_bq import write_late_buses_bq
from prefect import flow


@flow
def master_flow():

    live_buses = get_live_bus_locations()
    compare_bus_times(wait_for=[live_buses])
    write_late_buses_bq(wait_for=[compare_bus_times])


if __name__ == "__main__":

    master_flow()
