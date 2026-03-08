import csv
import pandas as pd
import os

INPUT_DIR = "parsed/clean"
STOP_TIMES = "gtfs/stop_times.txt"
TRIPS = "gtfs/trips.txt"
STOPS_FILE = "gtfs/stops.txt"

stop_rows = []
trip_rows = []

# -----------------------------
# Normalize station names
# -----------------------------
def normalize_station(name):
    s = str(name).upper().strip()
    s = s.replace("'", "")
    s = s.replace(".", "")
    s = s.replace("(", "")
    s = s.replace(")", "")
    s = s.replace("-", "")
    s = s.replace(" ", "")
    return s

# -----------------------------
# Load stops.txt
# -----------------------------
if os.path.exists(STOPS_FILE):
    stops_df = pd.read_csv(STOPS_FILE, encoding="latin1")
else:
    stops_df = pd.DataFrame(columns=[
        "stop_id","stop_name","stop_lat","stop_lon","zone_id","location_type"
    ])

stop_lookup = {}

for _, row in stops_df.iterrows():
    name = normalize_station(row["stop_name"])
    stop_lookup[name] = row["stop_id"]

print(f"Loaded {len(stop_lookup)} stops")

# store printed warnings only once
missing_stations = set()

# -----------------------------
# Station aliases
# -----------------------------
alias_map = {
    "MBAICENTRALL": "MUMBAICENTRAL",
    "MBAICENTRAL": "MUMBAICENTRAL",

    "MAHIMJN": "MAHIM",

    "PRABHADEVI": "ELPHINSTONEROAD",

    "LOWERPAREL": "LOWERPAREL",

    "MAHALAXMI": "MAHALAXMI",
    "MAHALAKSHMI": "MAHALAXMI",

    "SANTACRUZ": "SANTACRUZ",

    "NALLASOPARA": "NALLASOPARA",

    "VIRAR": "VIRAR"
}

# -----------------------------
# Process timetable CSV files
# -----------------------------
for file in sorted(os.listdir(INPUT_DIR)):

    if not file.endswith(".csv"):
        continue

    route_id = file.replace(".csv", "")
    route_id = route_id.replace("WR_timetable_", "WR_")

    filepath = os.path.join(INPUT_DIR, file)
    print(f"\nReading: {filepath}")

    df = pd.read_csv(filepath, encoding="latin1")

    station_col = None
    for candidate in ["station", "Station", "STATION"]:
        if candidate in df.columns:
            station_col = candidate
            break

    if station_col is None:
        station_col = df.columns[0]
        print(f"WARNING: using first column as station -> {station_col}")

    stations = df[station_col]
    train_columns = [c for c in df.columns if c != station_col]

    print(f"Stations: {len(stations)}, Trains: {len(train_columns)}")

    for train in train_columns:

        train = str(train).strip()

        if "unnamed" in train.lower():
            continue

        if not train or train.lower() == "nan":
            continue

        # find number of variants
        max_variants = 0

        for i in range(len(df)):
            cell = df.iloc[i][train]

            if pd.isna(cell):
                continue

            variants = str(cell).split("\n")
            max_variants = max(max_variants, len(variants))

        if max_variants == 0:
            continue

        # create trips
        for v in range(max_variants):
            prev_time= None

            trip_id = f"{train}_{v+1}"

            trip_rows.append([
                route_id,
                "DAILY",
                trip_id
            ])

            seq = 0

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

                # normalize time
                if len(t) == 5:
                    t = t + ":00"
                elif len(t) == 4:
                    t = "0" + t + ":00"

                station_name = normalize_station(stations.iloc[i])

                # apply alias
                if station_name in alias_map:
                    station_name = alias_map[station_name]

                # auto-create stop if missing
                if station_name not in stop_lookup:

                    if station_name not in missing_stations:
                        print(f"Adding missing stop -> {station_name}")
                        missing_stations.add(station_name)

                    stop_id = station_name

                    new_row = {
                        "stop_id": stop_id,
                        "stop_name": stations.iloc[i],
                        "stop_lat": "",
                        "stop_lon": "",
                        "zone_id": "",
                        "location_type": 0
                    }

                    stops_df = pd.concat(
                        [stops_df, pd.DataFrame([new_row])],
                        ignore_index=True
                    )

                    stop_lookup[station_name] = stop_id

                stop_id = stop_lookup[station_name]
                # prevent backwards time travel
                if prev_time and t < prev_time:
                    continue

                prev_time = t

                seq += 1

                stop_rows.append([
                    trip_id,
                    t,
                    t,
                    stop_id,
                    seq
])


os.makedirs("gtfs", exist_ok=True)

# convert to DataFrames
stop_df = pd.DataFrame(stop_rows, columns=[
    "trip_id",
    "arrival_time",
    "departure_time",
    "stop_id",
    "stop_sequence"
])

trip_df = pd.DataFrame(trip_rows, columns=[
    "route_id",
    "service_id",
    "trip_id"
])

# remove duplicates
stop_df = stop_df.drop_duplicates()
stop_df = stop_df.drop_duplicates(subset=["trip_id","stop_sequence"])

trip_df = trip_df.drop_duplicates()

# sort correctly
stop_df = stop_df.sort_values(["trip_id","stop_sequence"])

# save files
stop_df.to_csv(STOP_TIMES, index=False)
trip_df.to_csv(TRIPS, index=False)

# save updated stops
stops_df.drop_duplicates(subset=["stop_id"], inplace=True)
stops_df.to_csv(STOPS_FILE, index=False)

print("\nGTFS generated successfully")
print(f"stop_times rows: {len(stop_df)}")
print(f"trips rows: {len(trip_df)}")
print(f"stops rows: {len(stops_df)}")