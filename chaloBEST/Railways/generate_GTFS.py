import csv
import pandas as pd
import os
import re

INPUT_DIR = "parsed/clean"
STOP_TIMES = "gtfs/stop_times.txt"
TRIPS = "gtfs/trips.txt"
STOPS_FILE = "gtfs/stops.txt"

stop_rows = []
trip_rows = []

# -----------------------------
# Map CSV filename -> route_id in routes.txt
# -----------------------------
FILE_TO_ROUTE = {
    "CR_timetable_DN.csv": "CL",
    "CR_timetable_UP.csv": "CL",
    "HR_timetable_DN.csv": "HL",
    "HR_timetable_UP.csv": "HL",
    "WR_timetable_DN.csv": "WL",
    "WR_timetable_UP.csv": "WL",
}

# Map CSV filename -> direction_id (0=outbound, 1=inbound)
FILE_TO_DIRECTION = {
    "CR_timetable_DN.csv": 0,
    "CR_timetable_UP.csv": 1,
    "HR_timetable_DN.csv": 0,
    "HR_timetable_UP.csv": 1,
    "WR_timetable_DN.csv": 0,
    "WR_timetable_UP.csv": 1,
}

# Trip ID prefix to avoid duplicates across files
FILE_TO_PREFIX = {
    "CR_timetable_DN.csv": "CR_DN",
    "CR_timetable_UP.csv": "CR_UP",
    "HR_timetable_DN.csv": "HR_DN",
    "HR_timetable_UP.csv": "HR_UP",
    "WR_timetable_DN.csv": "WR_DN",
    "WR_timetable_UP.csv": "WR_UP",
}

# -----------------------------
# Coordinates for stops missing lat/lon
# -----------------------------
STOP_COORDS = {
    "VIRAR": (19.4550, 72.8110),
    "MAHALAXMI": (18.9830, 72.8220),
    "LOWERPAREL": (18.9940, 72.8310),
    "MAHIM": (19.0390, 72.8400),
    "KANDIVLI": (19.2040, 72.8530),
    "KCE": (19.0280, 72.8560),
    "KOPAR": (19.2160, 73.0460),
    "DOMBIVLI": (19.2183, 73.0867),
    "THAKURLI": (19.2350, 73.1050),
    "SHAHAD": (19.2480, 73.1280),
    "AMBIVLI": (19.2560, 73.1410),
    "TITWALA": (19.3020, 73.2050),
    "KHADAVLI": (19.3320, 73.2510),
    "VASIND": (19.4050, 73.2660),
    "ASANGAON": (19.4280, 73.3120),
    "ATGAON": (19.4820, 73.3350),
    "THANSIT": (19.5200, 73.3600),
    "KHARDI": (19.5510, 73.3750),
    "UMBERMALLI": (19.5780, 73.4000),
    "KELAVLI": (19.4300, 73.3500),
    "DOLAVLI": (19.4500, 73.3700),
    "LOWJEE": (19.4700, 73.3900),
    "KHOPOLI": (18.7870, 73.3420),
    "MSR": (19.0350, 73.0720),
}

# -----------------------------
# Normalize station names
# -----------------------------
def normalize_station(name):
    s = str(name).upper().strip()
    s = s.replace("'", "")
    s = s.replace(".", "")
    s = s.replace("(", "")
    s = s.replace(")", "")
    s = s.replace("-", " ")
    s = " ".join(s.split())  # collapse multiple spaces
    return s

# -----------------------------
# Station aliases: CSV name -> stop_id in stops.txt
# -----------------------------
ALIAS_TO_STOP_ID = {
    # Central line
    "CSMT": "CSMT",
    "MUMBAI CSMT": "CSMT",
    "CHHATRAPATI SHIVAJI MAHARAJ TERMINUS": "CSMT",
    "MASJID": "MSD",
    "SANDHURST ROAD": "SBR",
    "BYCULLA": "BY",
    "CHINCHPOKLI": "CRD",
    "CURREY ROAD": "CR",
    "PAREL": "PR",
    "DADAR": "DR",
    "MATUNGA": "MM",
    "SION": "SN",
    "KURLA": "KR",
    "VIDYAVIHAR": "VD",
    "GHATKOPAR": "GC",
    "VIKHROLI": "VK",
    "KANJURMARG": "KN",
    "BHANDUP": "BNR",
    "NAHUR": "NHP",
    "MULUND": "MNK",
    "THANE": "TNA",
    "KALWA": "KPR",
    "KALVA": "KPR",
    "MUMBRA": "MBQ",
    "DIVA": "DI",
    "DIVA JUNCTION": "DI",
    "KALYAN": "KYN",
    "KALYAN JUNCTION": "KYN",
    "VITHALWADI": "VTN",
    "ULHASNAGAR": "UBR",
    "AMBERNATH": "ABH",
    "BADLAPUR": "BDU",
    "VANGANI": "VGI",
    "SHELU": "SHD",
    "NERAL": "NRL",
    "BHIVPURI ROAD": "BVS",
    "KARJAT": "KJT",
    "PALASDHARI": "PLG",
    "KASARA": "KSR",

    # Western line
    "CHURCHGATE": "CCG",
    "MARINE LINES": "MR",
    "CHARNI ROAD": "CHR",
    "GRANT ROAD": "GT",
    "MUMBAI CENTRAL": "BC",
    "ELPHINSTONE ROAD": "ELR",
    "PRABHADEVI": "ELR",
    "LOWER PAREL": "LOWERPAREL",
    "MATUNGA ROAD": "MX",
    "MAHIM JN": "MAHIM",
    "MAHIM": "MAHIM",
    "MAHIM JUNCTION": "MAHIM",
    "BANDRA": "BA",
    "KHAR ROAD": "KHR",
    "SANTACRUZ": "STC",
    "VILE PARLE": "VLP",
    "VILEPARLE": "VLP",
    "ANDHERI": "AND",
    "JOGESHWARI": "JOS",
    "RAM MANDIR": "RAM",
    "RAMNAGAR": "RAM",
    "GOREGAON": "GOR",
    "MALAD": "MLD",
    "KANDIVALI": "KDV",
    "KANDIVLI": "KDV",
    "BORIVALI": "BSR",
    "DAHISAR": "DIC",
    "MIRA ROAD": "MIRA",
    "BHAYANDAR": "BYNR",
    "NAIGAON": "NVS",
    "VASAI ROAD": "VR",
    "NALLASOPARA": "NLB",
    "NALLA SOPARA": "NLB",
    "VIRAR": "VIRAR",
    "MAHALAXMI": "MAHALAXMI",
    "MAHALAKSHMI": "MAHALAXMI",

    # Harbour line
    "DOCKYARD ROAD": "DCK",
    "REAY ROAD": "RYR",
    "COTTON GREEN": "CTN",
    "SEWRI": "SWR",
    "VADALA ROAD": "VDB",
    "GTB NAGAR": "GTR",
    "CHUNABHATTI": "CLA",
    "TILAK NAGAR": "TLK",
    "TILAKNAGAR": "TLK",
    "CHEMBUR": "CDB",
    "GOVANDI": "GVD",
    "MANKHURD": "MBE",
    "VASHI": "VBS",
    "SANPADA": "SNV",
    "JUINAGAR": "JNR",
    "NERUL": "NMB",
    "SEAWOOD DARAVE": "SWD",
    "SEAWOODS DARAVE": "SWD",
    "BELAPUR CBD": "CBD",
    "CBD BELAPUR": "CBD",
    "KHARGHAR": "KBE",
    "KHANDESHWAR": "KHP",
    "MANSAROVAR": "MSR",
    "PANVEL": "PNV",
    "KINGS CIRCLE": "KCE",

    # Stations beyond typical suburban
    "KOPAR": "KOPAR",
    "DOMBIVLI": "DOMBIVLI",
    "THAKURLI": "THAKURLI",
    "SHAHAD": "SHAHAD",
    "AMBIVLI": "AMBIVLI",
    "TITWALA": "TITWALA",
    "KHADAVLI": "KHADAVLI",
    "VASIND": "VASIND",
    "ASANGAON": "ASANGAON",
    "ATGAON": "ATGAON",
    "THANSIT": "THANSIT",
    "KHARDI": "KHARDI",
    "UMBERMALLI": "UMBERMALLI",
    "KELAVLI": "KELAVLI",
    "DOLAVLI": "DOLAVLI",
    "LOWJEE": "LOWJEE",
    "KHOPOLI": "KHOPOLI",
}


def resolve_stop_id(station_raw, stop_lookup):
    normalized = normalize_station(station_raw)

    if normalized in ALIAS_TO_STOP_ID:
        return ALIAS_TO_STOP_ID[normalized]

    if normalized in stop_lookup:
        return stop_lookup[normalized]

    no_space = normalized.replace(" ", "")
    for key, sid in ALIAS_TO_STOP_ID.items():
        if key.replace(" ", "") == no_space:
            return sid

    for key, sid in ALIAS_TO_STOP_ID.items():
        if no_space in key.replace(" ", "") or key.replace(" ", "") in no_space:
            return sid

    stop_id = normalized.replace(" ", "_")
    if stop_id not in stop_lookup:
        stop_lookup[normalized] = stop_id
        print(f"  NEW STOP: '{station_raw}' -> {stop_id}")

    return stop_id


# -----------------------------
# Load stops.txt
# -----------------------------
if os.path.exists(STOPS_FILE):
    stops_df = pd.read_csv(STOPS_FILE, encoding="latin1")
else:
    stops_df = pd.DataFrame(columns=[
        "stop_id", "stop_name", "stop_lat", "stop_lon", "location_type", "parent_station", "zone_id"
    ])

stop_lookup = {}
for _, row in stops_df.iterrows():
    name = normalize_station(row["stop_name"])
    stop_lookup[name] = row["stop_id"]

stop_ids_existing = set(stops_df["stop_id"].astype(str).values)

print(f"Loaded {len(stop_lookup)} stops from stops.txt")

new_stops = []

# -----------------------------
# Process timetable CSV files
# -----------------------------
for file in sorted(os.listdir(INPUT_DIR)):

    if not file.endswith(".csv"):
        continue

    if file.startswith("._"):
        continue

    # Use proper route_id from mapping
    route_id = FILE_TO_ROUTE.get(file)
    direction_id = FILE_TO_DIRECTION.get(file, 0)
    trip_prefix = FILE_TO_PREFIX.get(file, file.replace(".csv", ""))

    if route_id is None:
        print(f"  SKIP unknown file: {file}")
        continue

    filepath = os.path.join(INPUT_DIR, file)
    print(f"\nReading: {filepath} (route={route_id}, direction={direction_id})")

    df = pd.read_csv(filepath, encoding="latin1")

    station_col = None
    for candidate in ["station", "Station", "STATION"]:
        if candidate in df.columns:
            station_col = candidate
            break

    if station_col is None:
        station_col = df.columns[0]
        print(f"  WARNING: using first column as station -> '{station_col}'")

    stations = df[station_col]
    train_columns = [c for c in df.columns if c != station_col]

    train_columns = [
        c for c in train_columns
        if str(c).strip()
        and "unnamed" not in str(c).lower()
        and str(c).strip().lower() != "nan"
    ]

    print(f"  Stations: {len(stations)}, Trains: {len(train_columns)}")

    for train_col in train_columns:

        train = str(train_col).strip()

        if not re.match(r"^\d{4,6}$", train):
            continue

        max_variants = 0
        for i in range(len(df)):
            cell = df[train_col].iloc[i] if train_col in df.columns else df.iloc[i][train_col]
            if pd.isna(cell):
                continue
            variants = str(cell).split("\n")
            valid_variants = [v for v in variants if re.match(r"^\d{2}:\d{2}$", v.strip())]
            max_variants = max(max_variants, len(valid_variants))

        if max_variants == 0:
            continue

        for v in range(max_variants):
            # Prefix trip_id with direction to avoid duplicates across DN/UP
            trip_id = f"{trip_prefix}_{train}_{v + 1}"
            trip_rows.append([route_id, "DAILY", trip_id, direction_id])

            seq = 0
            prev_minutes = -1

            for i in range(len(df)):
                cell = df[train_col].iloc[i] if train_col in df.columns else df.iloc[i][train_col]
                if pd.isna(cell):
                    continue

                variants = str(cell).split("\n")
                valid_variants = [vv for vv in variants if re.match(r"^\d{2}:\d{2}$", vv.strip())]

                if v >= len(valid_variants):
                    continue

                t = valid_variants[v].strip()

                h, m = t.split(":")
                cur_minutes = int(h) * 60 + int(m)

                if cur_minutes < prev_minutes:
                    continue

                prev_minutes = cur_minutes

                time_str = f"{t}:00"

                station_raw = str(stations.iloc[i])
                stop_id = resolve_stop_id(station_raw, stop_lookup)

                if re.match(r"^\d{2}:\d{2}", stop_id):
                    continue

                seq += 1
                stop_rows.append([trip_id, time_str, time_str, stop_id, seq])


# -----------------------------
# Output
# -----------------------------
os.makedirs("gtfs", exist_ok=True)

stop_df = pd.DataFrame(stop_rows, columns=[
    "trip_id", "arrival_time", "departure_time", "stop_id", "stop_sequence"
])

trip_df = pd.DataFrame(trip_rows, columns=[
    "route_id", "service_id", "trip_id", "direction_id"
])

# Remove trips with < 2 stops (invalid in GTFS)
valid_trips = stop_df.groupby("trip_id").filter(lambda x: len(x) >= 2)["trip_id"].unique()
stop_df = stop_df[stop_df["trip_id"].isin(valid_trips)]
trip_df = trip_df[trip_df["trip_id"].isin(valid_trips)]

# Remove duplicates
stop_df = stop_df.drop_duplicates(subset=["trip_id", "stop_sequence"])
trip_df = trip_df.drop_duplicates(subset=["trip_id"])

# Sort
stop_df = stop_df.sort_values(["trip_id", "stop_sequence"])

# Write
stop_df.to_csv(STOP_TIMES, index=False)
trip_df.to_csv(TRIPS, index=False)

# Update stops.txt with any new stops
# Collect all stop_ids actually used
used_stop_ids = set(stop_df["stop_id"].unique())

# Add missing stops to stops_df
for sid in used_stop_ids:
    if sid not in stop_ids_existing:
        if not stops_df["stop_id"].eq(sid).any():
            lat, lon = STOP_COORDS.get(sid, ("", ""))
            new_stops.append({
                "stop_id": sid,
                "stop_name": sid.replace("_", " ").title(),
                "stop_lat": lat,
                "stop_lon": lon,
                "location_type": 0,
                "parent_station": "",
                "zone_id": ""
            })

# Also fill in missing coordinates for existing stops
for idx, row in stops_df.iterrows():
    sid = str(row["stop_id"])
    if (pd.isna(row.get("stop_lat")) or str(row.get("stop_lat")).strip() == "") and sid in STOP_COORDS:
        lat, lon = STOP_COORDS[sid]
        stops_df.at[idx, "stop_lat"] = lat
        stops_df.at[idx, "stop_lon"] = lon

if new_stops:
    stops_df = pd.concat([stops_df, pd.DataFrame(new_stops)], ignore_index=True)

stops_df.drop_duplicates(subset=["stop_id"], inplace=True)
stops_df.to_csv(STOPS_FILE, index=False)

print(f"\nGTFS generated successfully")
print(f"  stop_times: {len(stop_df)} rows")
print(f"  trips: {len(trip_df)} rows")
print(f"  stops: {len(stops_df)} rows")
print(f"  new stops added: {len(new_stops)}")