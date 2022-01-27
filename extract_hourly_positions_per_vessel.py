import os
import csv
import json
import glob
import requests
from dateutil import parser
from datetime import datetime, timedelta

API_KEY = "REPLACE_WITH_YOUR_API_TOKEN"


def round_time_to_hour(t):
    # Rounds to nearest hour by adding a timedelta hour if minute >= 30
    return t.replace(second=0, microsecond=0, minute=0, hour=t.hour) + timedelta(
        hours=t.minute // 30
    )


def get_hourly_positions(filename):
    responses = []
    datafile = os.path.join(os.path.dirname(__file__), filename)
    csv_file = open(datafile)
    reader = csv.DictReader(csv_file)
    # i = 0
    vessels = {}
    for row in reader:
        mmsi = row["mmsi"]
        if mmsi not in vessels:
            vessels[mmsi] = [row]
        else:
            vessels[mmsi].append(row)
    waypoint_count = 0
    # hourly_positions = {} # for combined payload
    for mmsi, rows in vessels.items():
        hourly_positions = {}  # for individual MMSI payloads
        for row in rows:
            timestamp = parser.parse(row["position_timestamp"])
            rounded_time = round_time_to_hour(timestamp)
            time_string = str(rounded_time)
            hpkey = "{}_{}".format(mmsi, time_string)
            # check if position data has already been stored for this hour
            if hpkey not in hourly_positions:
                # we have not stored position data yet for this rounded hour
                # so we simply copy the data
                hourly_positions[hpkey] = row
            elif hpkey in hourly_positions:
                # we have aready stored position data for this rounded hour
                # so we need to decide whether or not to replace it
                # with the current row's position data
                previous_time = hourly_positions[hpkey]["position_timestamp"]
                previous_time = parser.parse(previous_time)
                # calculate time deltas for the previous and current data rows
                # to determine how close they are to the rounded hour
                previous_difference = abs(previous_time - rounded_time)
                current_difference = abs(timestamp - rounded_time)
                # only replace the existing position data for this hour
                # if the current timestamp is closer to the hour
                if current_difference < previous_difference:
                    hourly_positions[hpkey] = row
            # print progress for script user
            # i += 1
            # print("Processed row {} for {}".format(i, filename))
        outfile = "{}_{}".format(mmsi, filename)
        # log the number of waypoints
        waypoints = len(hourly_positions)
        print("MMSI:{}, Waypoints:{}".format(mmsi, waypoints))
        waypoint_count += waypoints
        # # write new data to CSV
        # write_output(outfile, hourly_positions)
        # convert to Historical Trajectory API input format
        payload = generate_trajectory_api_payload(outfile, hourly_positions)
        #################################
        #################################
        ### THE POINT OF NO RETURN
        #################################
        #################################
        # response = make_api_request(mmsi, payload)
        # responses.append(response)

    # print("\nTotal Waypoints:", waypoint_count)
    with open("RESPONSES.json", "w") as outfile:
        json.dump(responses, outfile)


def generate_trajectory_api_payload(filename, hourly_positions):
    # name = filename.split("/")[1]
    mmsi = filename.split("_")[0]
    payload = {"name": mmsi, "waypoints": []}
    for hpkey, data in hourly_positions.items():
        # remove MMSI from timestring
        rounded_time = hpkey.split("_")[1]
        # remove timezone suffix
        time = rounded_time.split("+")[0]
        # replace space with T character
        time = time.replace(" ", "T")
        lat = data["latitude"]
        lon = data["longitude"]
        if lat is not "" and lon is not "":
            waypoint = {
                "lat": float(lat),
                "lon": float(lon),
                "time": time,
            }
            payload["waypoints"].append(waypoint)
    # with open("{}_TRAJECTORY_API_PAYLOAD.json".format(mmsi), "w") as outfile:
    #     json.dump(payload, outfile)
    return payload


# def write_output(filename, hourly_positions):
#     # outname = filename.split("/")[-1]
#     outname = "hourly-positions-" + filename
#     with open(outname, "w") as outfile:
#         fieldnames = [
#             "latitude",
#             "longitude",
#             "position_timestamp",
#             "mmsi",
#             "imo",
#             "rounded_time",
#         ]
#         writer = csv.DictWriter(outfile, fieldnames=fieldnames)
#         # write the header first
#         writer.writeheader()
#         for rounded_time, data in hourly_positions.items():
#             row = {}
#             row["mmsi"] = data["mmsi"]
#             row["imo"] = data["imo"]
#             row["position_timestamp"] = data["position_timestamp"]
#             row["rounded_time"] = rounded_time
#             row["latitude"] = data["latitude"]
#             row["longitude"] = data["longitude"]
#             # write the data to a new CSV row
#             writer.writerow(row)
#         # print("Saved to ", outname)


def make_api_request(mmsi, route):
    url = "https://api.wx.spire.com/archive/route"
    headers = {
        "spire-api-key": API_KEY,
        "Content-Type": "application/json",
    }
    body = {
        "output_format": "CSV",
        "fields": [
            "wind_gust",
            "max_wind_gust",
            "eastward_wind",
            "northward_wind",
            "wind_speed",
            "wind_direction",
            "sea_surface_temperature",
            "sea_surface_wave_significant_height",
            "sea_surface_wave_mean_direction",
            "sea_surface_wave_mean_period",
            "sea_surface_wind_wave_significant_height",
            "sea_surface_wind_wave_mean_direction",
            "sea_surface_wind_wave_mean_period",
            "sea_surface_total_swell_wave_significant_height",
            "sea_surface_total_swell_wave_mean_direction",
            "sea_surface_total_swell_wave_mean_period",
            "sea_surface_first_partition_swell_wave_significant_height",
            "sea_surface_first_partition_swell_wave_mean_direction",
            "sea_surface_first_partition_swell_wave_mean_period",
            "sea_surface_second_partition_swell_wave_significant_height",
            "sea_surface_second_partition_swell_wave_mean_direction",
            "sea_surface_second_partition_swell_wave_mean_period",
            "sea_surface_maximum_individual_wave_height",
            "sea_ice_cover",
            "eastward_sea_water_velocity",
            "northward_sea_water_velocity",
            "sea_water_speed",
            "sea_water_direction",
        ],
        "invariant_fields": ["land_sea_mask", "model_bathymetry"],
        "route": route,
    }
    payload = json.dumps(body)
    print("Making API request for {}".format(mmsi))
    # print(payload)
    response = requests.request("POST", url, headers=headers, data=payload)
    resp = json.loads(response.text)
    return resp


if __name__ == "__main__":
    filenames = glob.glob("YOUR_DATA_*.csv")
    for filename in filenames:
        # replace all timestamps with a rounded hourly timestamp
        hourly_positions = get_hourly_positions(filename)
