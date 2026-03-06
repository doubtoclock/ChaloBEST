import csv
import pandas as pd
import os

INPUT_DIR = "parsed/clean"
STOP_TIMES = "gtfs/stop_times.txt"
TRIPS = "gtfs/trips.txt"

stop_rows = []
trip_rows = []

def clean_stop_id(name):
    s = name.upper()
    s = s.replace(" ", "_")
    s = s.replace("'", "")
    s = s.replace(".", "")
    s = s.replace("(", "")
    s = s.replace(")", "")
    return s


for file in os.listdir(INPUT_DIR):

    if not file.endswith(".csv"):
        continue

    route_id = file.replace(".csv", "")
    route_id = route_id.replace("WR_timetable_", "WR_")

    df = pd.read_csv(os.path.join(INPUT_DIR, file))
    stations = df["station"]

    for train in df.columns[1:]:

        train = str(train).strip()

        if not train:
            continue

        # find max variants
        max_variants = 0

        for i in range(len(df)):
            cell = df.iloc[i][train]

            if pd.isna(cell):
                continue

            variants = str(cell).split("\n")
            max_variants = max(max_variants, len(variants))

        # create trips
        for v in range(max_variants):

            trip_id = f"{train}_{v+1}"
            trip_rows.append([route_id, "DAILY", trip_id])

            seq = 1

            for i in range(len(df)):

                cell = df.iloc[i][train]

                if pd.isna(cell):
                    continue

                variants = str(cell).split("\n")

                if v >= len(variants):
                    continue

                t = variants[v].strip()

                if ":" not in t:
                    continue

                if len(t) == 5:
                    t = t + ":00"

                stop_id = clean_stop_id(stations[i])

                stop_rows.append([
                    trip_id,
                    t,
                    t,
                    stop_id,
                    seq
                ])

                seq += 1


os.makedirs("gtfs", exist_ok=True)

# stop_times
with open(STOP_TIMES, "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow([
        "trip_id",
        "arrival_time",
        "departure_time",
        "stop_id",
        "stop_sequence"
    ])
    writer.writerows(stop_rows)

# trips
with open(TRIPS, "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["route_id","service_id","trip_id"])
    writer.writerows(trip_rows)

print("GTFS generated successfully")