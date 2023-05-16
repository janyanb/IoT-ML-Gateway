import influxdb_client
import os
import time
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

# token = os.environ.get("INFLUXDB_TOKEN")
token = "c1DGKoLNjL1Lo3jMCVw3X4Gw__EDzMKnlAbR6K4w2GHeBcwaaqwPMlzbbue4LHkC6-dhYnH_UfzGS-C5zInOXw=="
org = "Project 2023"
url = "http://localhost:8086"
bucket = "WindTurbine Data"

write_client = influxdb_client.InfluxDBClient(url=url, token=token, org=org)
write_api = write_client.write_api(write_options=SYNCHRONOUS)

for value in range(5):
    point = (
        Point("measurement1")
        .tag("tagname1", "tagvalue1")
        .field("field1", value)
    )
    write_api.write(bucket=bucket, org="Project 2023", record=point)
