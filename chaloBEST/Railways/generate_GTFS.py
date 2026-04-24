import os
import re
import zipfile
from datetime import date

import pandas as pd # type: ignore

INPUT_DIR = "parsed/csv"
GTFS_DIR = "gtfs"
STOP_TIMES = os.path.join(GTFS_DIR, "stop_times.txt")
TRIPS = os.path.join(GTFS_DIR, "trips.txt")
STOPS_FILE = os.path.join(GTFS_DIR, "stops.txt")
ROUTES_FILE = os.path.join(GTFS_DIR, "routes.txt")
CALENDAR_FILE = os.path.join(GTFS_DIR, "calendar.txt")
FEED_INFO_FILE = os.path.join(GTFS_DIR, "feed_info.txt")
GTFS_ZIP = os.path.join(GTFS_DIR, "gtfs.zip")
INCLUDE_SHAPES = False

TRAIN_PATTERN = re.compile(r"^\d{4,6}[A-Z]?$")
TIME_PATTERN = re.compile(r"\b\d{2}:\d{2}\b")

stop_rows = []
trip_rows = []
stop_name_by_id = {}

FILE_TO_ROUTE = {
    "CR_timetable_DN.csv": "CL",
    "CR_timetable_UP.csv": "CL",
    "HR_timetable_DN.csv": "HL",
    "HR_timetable_UP.csv": "HL",
    "WR_timetable_DN.csv": "WL",
    "WR_timetable_UP.csv": "WL",
}

FILE_TO_DIRECTION = {
    "CR_timetable_DN.csv": 0,
    "CR_timetable_UP.csv": 1,
    "HR_timetable_DN.csv": 0,
    "HR_timetable_UP.csv": 1,
    "WR_timetable_DN.csv": 0,
    "WR_timetable_UP.csv": 1,
}

FILE_TO_PREFIX = {
    "CR_timetable_DN.csv": "CR_DN",
    "CR_timetable_UP.csv": "CR_UP",
    "HR_timetable_DN.csv": "HR_DN",
    "HR_timetable_UP.csv": "HR_UP",
    "WR_timetable_DN.csv": "WR_DN",
    "WR_timetable_UP.csv": "WR_UP",
}

ROUTE_METADATA = {
    "CL": {
        "agency_id": "CR",
        "route_short_name": "Central",
        "route_long_name": "Mumbai Suburban Railway Central Line",
        "route_type": 2,
        "route_color": "C62828",
    },
    "HL": {
        "agency_id": "CR",
        "route_short_name": "Harbour",
        "route_long_name": "Mumbai Suburban Railway Harbour Line",
        "route_type": 2,
        "route_color": "2E7D32",
    },
    "WL": {
        "agency_id": "WR",
        "route_short_name": "Western",
        "route_long_name": "Mumbai Suburban Railway Western Line",
        "route_type": 2,
        "route_color": "1565C0",
    },
}

SHAPE_BY_ROUTE = {
    "CL": "shape_CL",
    "HL": "shape_HL",
    "WL": "shape_WL",
}

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
    "BDU": (19.1663883, 73.2390496),
    "VGI": (19.0941667, 73.3011111),
    "SHD": (19.0629850, 73.3173860),
    "NRL": (19.0271130, 73.3189550),
    "BVS": (18.9706550, 73.3313240),
    "KJT": (18.9111580, 73.3206910),
    "PLG": (18.8840900, 73.3205500),
    "KELAVLI": (18.8581580, 73.3182800),
    "DOLAVLI": (18.8446750, 73.3188380),
    "LOWJEE": (18.8099550, 73.3350060),
    "KHOPOLI": (18.7884484, 73.3460214),
    "MSR": (19.0350, 73.0720),
}


def normalize_station(name):
    s = str(name).upper().strip()
    s = s.replace("'", "")
    s = s.replace(".", "")
    s = s.replace("(", "")
    s = s.replace(")", "")
    s = s.replace("-", " ")
    s = " ".join(s.split())
    return s


ALIAS_TO_STOP_ID = {
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
    "SANTA CRUZ": "STC",
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
    "UMBERMALI": "UMBERMALLI",
    "UMBERMALLI": "UMBERMALLI",
    "KELAVLI": "KELAVLI",
    "DOLAVLI": "DOLAVLI",
    "LOWJEE": "LOWJEE",
    "KHOPOLI": "KHOPOLI",
}

CENTRAL_DN_ORDER = [
    "TRAIN",
    "CSMT",
    "Masjid",
    "Sandhurst Road",
    "Byculla",
    "Chinchpokli",
    "Currey Road",
    "Parel",
    "Dadar",
    "Matunga",
    "Sion",
    "Kurla",
    "Vidyavihar",
    "Ghatkopar",
    "Vikhroli",
    "Kanjurmarg",
    "Bhandup",
    "Nahur",
    "Mulund",
    "Thane",
    "Kalwa",
    "Mumbra",
    "Diva Junction",
    "Kopar",
    "Dombivli",
    "Thakurli",
    "Kalyan",
    "Shahad",
    "Ambivli",
    "Titwala",
    "Khadavli",
    "Vasind",
    "Asangaon",
    "Atgaon",
    "Thansit",
    "Khardi",
    "Umbermali",
    "Kasara",
    "Vithalwadi",
    "Ulhasnagar",
    "Ambernath",
    "Badlapur",
    "Vangani",
    "Shelu",
    "Neral",
    "Bhivpuri Road",
    "Karjat",
    "Palasdhari",
    "Kelavli",
    "Dolavli",
    "Lowjee",
    "Khopoli",
]

CENTRAL_UP_ORDER = [
    "TRAIN",
    "Kasara",
    "Umbermali",
    "Khardi",
    "Thansit",
    "Atgaon",
    "Asangaon",
    "Vasind",
    "Khadavli",
    "Titwala",
    "Ambivli",
    "Shahad",
    "Khopoli",
    "Lowjee",
    "Dolavli",
    "Kelavli",
    "Palasdhari",
    "Karjat",
    "Bhivpuri Road",
    "Neral",
    "Shelu",
    "Vangani",
    "Badlapur",
    "Ambernath",
    "Ulhasnagar",
    "Vithalwadi",
    "Kalyan",
    "Thakurli",
    "Dombivli",
    "Kopar",
    "Diva Junction",
    "Mumbra",
    "Kalwa",
    "Thane",
    "Mulund",
    "Nahur",
    "Bhandup",
    "Kanjurmarg",
    "Vikhroli",
    "Ghatkopar",
    "Vidyavihar",
    "Kurla",
    "Sion",
    "Matunga",
    "Dadar",
    "Parel",
    "Currey Road",
    "Chinchpokli",
    "Byculla",
    "Sandhurst Road",
    "Masjid",
    "CSMT",
]

STOP_INTERPOLATION_SEQUENCES = [
    ["CSMT", "MSD", "SBR", "BY", "CRD", "CR", "PR", "DR", "MM", "SN", "KR", "VD", "GC", "VK", "KN", "BNR", "NHP", "MNK", "TNA", "KPR", "MBQ", "DI", "KOPAR", "DOMBIVLI", "THAKURLI", "KYN"],
    ["KYN", "SHAHAD", "AMBIVLI", "TITWALA", "KHADAVLI", "VASIND", "ASANGAON", "ATGAON", "THANSIT", "KHARDI", "UMBERMALLI", "KSR"],
    ["KYN", "VTN", "UBR", "ABH", "BDU", "VGI", "SHD", "NRL", "BVS", "KJT", "PLG", "KELAVLI", "DOLAVLI", "LOWJEE", "KHOPOLI"],
    ["CCG", "MR", "CHR", "GT", "BC", "MAHALAXMI", "LOWERPAREL", "ELR", "DR", "MX", "MAHIM", "BA", "KHR", "STC", "VLP", "AND", "JOS", "RAM", "GOR", "MLD", "KDV", "BSR", "DIC", "MIRA", "BYNR", "NVS", "VR", "NLB", "VIRAR"],
    ["CSMT", "MSD", "SBR", "DCK", "RYR", "CTN", "SWR", "VDB", "KCE", "MAHIM", "BA", "KHR", "STC", "VLP", "AND", "JOS", "RAM", "GOR"],
    ["CSMT", "MSD", "SBR", "DCK", "RYR", "CTN", "SWR", "VDB", "GTR", "CLA", "TLK", "CDB", "GVD", "MBE", "VBS", "SNV", "JNR", "NMB", "SWD", "CBD", "KBE", "KHP", "MSR", "PNV"],
]

CANONICAL_STOP_NAMES = {
    "ASANGAON": "Asangaon",
    "BDU": "Badlapur",
    "BNR": "Bhandup",
    "BVS": "Bhivpuri Road",
    "BY": "Byculla",
    "CR": "Currey Road",
    "DI": "Diva Junction",
    "DOMBIVLI": "Dombivli",
    "DR": "Dadar",
    "KOPAR": "Kopar",
    "KPR": "Kalwa",
    "KYN": "Kalyan",
    "MNK": "Mulund",
    "SHAHAD": "Shahad",
    "SN": "Sion",
    "STC": "Santa Cruz",
    "SWD": "Seawoods Darave",
    "THANSIT": "Thansit",
    "VD": "Vidyavihar",
    "VK": "Vikhroli",
    "VLP": "Vile Parle",
}


def extract_times(cell):
    return TIME_PATTERN.findall(str(cell))


def hhmm_to_minutes(value):
    hour, minute = value.split(":")
    return int(hour) * 60 + int(minute)


def format_gtfs_time(total_minutes):
    hours, minutes = divmod(total_minutes, 60)
    return f"{hours:02d}:{minutes:02d}:00"


def adjust_for_rollover(current_minutes, previous_total):
    total = current_minutes
    while previous_total is not None and total < previous_total:
        total += 24 * 60
    return total


def resolve_stop_id(station_raw, stop_lookup):
    normalized = normalize_station(station_raw)

    if normalized in ALIAS_TO_STOP_ID:
        stop_id = ALIAS_TO_STOP_ID[normalized]
        stop_lookup.setdefault(normalized, stop_id)
        return stop_id

    if normalized in stop_lookup:
        return stop_lookup[normalized]

    no_space = normalized.replace(" ", "")
    for key, stop_id in ALIAS_TO_STOP_ID.items():
        if key.replace(" ", "") == no_space:
            stop_lookup[normalized] = stop_id
            return stop_id

    for key, stop_id in ALIAS_TO_STOP_ID.items():
        compact = key.replace(" ", "")
        if no_space in compact or compact in no_space:
            stop_lookup[normalized] = stop_id
            return stop_id

    stop_id = normalized.replace(" ", "_")
    stop_lookup[normalized] = stop_id
    print(f"  NEW STOP: '{station_raw}' -> {stop_id}")
    return stop_id


def clean_value(value):
    if pd.isna(value):
        return ""
    return str(value).strip()


def load_csv(path):
    return pd.read_csv(path, encoding="latin1")


def is_ignored_column(name):
    label = str(name).strip()
    return not label or "unnamed" in label.lower() or label.lower() == "nan"


def detect_layout(df):
    if any(str(col).strip().upper() == "TRAIN" for col in df.columns):
        return "train_rows"

    other_cols = [col for col in df.columns[1:] if not is_ignored_column(col)]
    if not other_cols:
        return "unknown"

    train_like = sum(bool(TRAIN_PATTERN.match(str(col).strip())) for col in other_cols)
    return "station_rows" if train_like >= len(other_cols) / 2 else "train_rows"


def ensure_column(df, name):
    if name not in df.columns:
        df[name] = ""
    return df


def has_nonblank(row, columns):
    for column in columns:
        if column in row.index and clean_value(row[column]):
            return True
    return False


def reorder_columns(df, ordered_columns):
    ordered = [col for col in ordered_columns if col in df.columns]
    remainder = [col for col in df.columns if col not in ordered]
    return df[ordered + remainder]


def normalize_central_dataframe(file, df):
    df = df.copy()
    df = ensure_column(df, "Umbermali")
    if "Umbermalli" in df.columns:
        df["Umbermali"] = df["Umbermali"].where(df["Umbermali"].astype(str).str.strip().ne(""), df["Umbermalli"])
        df = df.drop(columns=["Umbermalli"])

    if file == "CR_timetable_DN.csv":
        df = ensure_column(df, "Shahad")
        khopoli_only = ["Palasdhari", "Kelavli", "Dolavli", "Lowjee"]
        kasara_branch = ["Ambivli", "Titwala", "Khadavli", "Vasind", "Asangaon", "Atgaon", "Thansit", "Khardi", "Umbermali", "Kasara"]

        for idx, row in df.iterrows():
            khopoli_value = clean_value(row.get("Khopoli", ""))
            if khopoli_value and has_nonblank(row, kasara_branch) and not has_nonblank(row, khopoli_only):
                df.at[idx, "Shahad"] = khopoli_value
                df.at[idx, "Khopoli"] = ""

        return reorder_columns(df, CENTRAL_DN_ORDER)

    if file == "CR_timetable_UP.csv":
        df = ensure_column(df, "Khopoli")
        khopoli_branch = ["Lowjee", "Dolavli", "Kelavli", "Palasdhari", "Karjat"]
        kasara_only = ["Kasara", "Umbermali", "Khardi", "Thansit", "Atgaon", "Asangaon", "Vasind", "Khadavli", "Titwala", "Ambivli"]

        for idx, row in df.iterrows():
            shahad_value = clean_value(row.get("Shahad", ""))
            if shahad_value and has_nonblank(row, khopoli_branch) and not has_nonblank(row, kasara_only):
                df.at[idx, "Khopoli"] = shahad_value
                df.at[idx, "Shahad"] = ""

        return reorder_columns(df, CENTRAL_UP_ORDER)

    return df


def append_trip(route_id, direction_id, trip_id, station_time_pairs, stop_lookup):
    if len(station_time_pairs) < 2:
        return

    shape_id = SHAPE_BY_ROUTE.get(route_id, "") if INCLUDE_SHAPES else ""
    trip_rows.append([route_id, "DAILY", trip_id, direction_id, shape_id])

    previous_total = None
    for seq, (station_name, time_value) in enumerate(station_time_pairs, start=1):
        total_minutes = adjust_for_rollover(hhmm_to_minutes(time_value), previous_total)
        previous_total = total_minutes
        time_str = format_gtfs_time(total_minutes)
        stop_id = resolve_stop_id(station_name, stop_lookup)
        stop_name_by_id.setdefault(stop_id, str(station_name).strip())
        stop_rows.append([trip_id, time_str, time_str, stop_id, seq])


def process_train_rows(df, route_id, direction_id, trip_prefix, stop_lookup):
    train_col = next((col for col in df.columns if str(col).strip().upper() == "TRAIN"), df.columns[0])
    station_columns = [col for col in df.columns if col != train_col and not is_ignored_column(col)]

    print(f"  Layout: train_rows | Trains: {len(df)}, Stations: {len(station_columns)}")

    for _, row in df.iterrows():
        train = str(row[train_col]).strip()
        if not TRAIN_PATTERN.match(train):
            continue

        variants_by_station = {station: extract_times(row[station]) for station in station_columns}
        max_variants = max((len(times) for times in variants_by_station.values()), default=0)

        for variant_idx in range(max_variants):
            station_time_pairs = []
            for station in station_columns:
                times = variants_by_station[station]
                if variant_idx < len(times):
                    station_time_pairs.append((station, times[variant_idx]))

            append_trip(
                route_id,
                direction_id,
                f"{trip_prefix}_{train}_{variant_idx + 1}",
                station_time_pairs,
                stop_lookup,
            )


def process_station_rows(df, route_id, direction_id, trip_prefix, stop_lookup):
    station_col = next(
        (col for col in df.columns if str(col).strip().upper() == "STATION"),
        df.columns[0],
    )

    stations = df[station_col].fillna("").astype(str)
    train_columns = [
        col
        for col in df.columns
        if col != station_col and not is_ignored_column(col) and TRAIN_PATTERN.match(str(col).strip())
    ]

    print(f"  Layout: station_rows | Stations: {len(stations)}, Trains: {len(train_columns)}")

    for train_col in train_columns:
        train = str(train_col).strip()
        max_variants = max((len(extract_times(cell)) for cell in df[train_col]), default=0)

        for variant_idx in range(max_variants):
            station_time_pairs = []
            for idx, station_raw in enumerate(stations):
                times = extract_times(df.at[idx, train_col])
                if variant_idx < len(times):
                    station_time_pairs.append((station_raw, times[variant_idx]))

            append_trip(
                route_id,
                direction_id,
                f"{trip_prefix}_{train}_{variant_idx + 1}",
                station_time_pairs,
                stop_lookup,
            )


def load_existing_stops():
    if os.path.exists(STOPS_FILE) and os.path.getsize(STOPS_FILE) > 0:
        try:
            return pd.read_csv(STOPS_FILE, encoding="latin1")
        except pd.errors.EmptyDataError:
            pass

    return pd.DataFrame(
        columns=[
            "stop_id",
            "stop_name",
            "stop_lat",
            "stop_lon",
            "location_type",
            "parent_station",
            "zone_id",
        ]
    )


def write_routes_file(route_ids):
    routes_df = pd.DataFrame(
        [
            {
                "route_id": route_id,
                **ROUTE_METADATA[route_id],
            }
            for route_id in sorted(route_ids)
            if route_id in ROUTE_METADATA
        ],
        columns=[
            "route_id",
            "agency_id",
            "route_short_name",
            "route_long_name",
            "route_type",
            "route_color",
        ],
    )
    routes_df.to_csv(ROUTES_FILE, index=False)


def write_calendar_file(service_ids):
    today = date.today()
    start_date = f"{today.year}0101"
    end_date = f"{today.year + 1}1231"

    rows = []
    for service_id in sorted(service_ids):
        if service_id == "DAILY":
            rows.append(
                {
                    "service_id": service_id,
                    "monday": 1,
                    "tuesday": 1,
                    "wednesday": 1,
                    "thursday": 1,
                    "friday": 1,
                    "saturday": 1,
                    "sunday": 1,
                    "start_date": start_date,
                    "end_date": end_date,
                }
            )

    calendar_df = pd.DataFrame(
        rows,
        columns=[
            "service_id",
            "monday",
            "tuesday",
            "wednesday",
            "thursday",
            "friday",
            "saturday",
            "sunday",
            "start_date",
            "end_date",
        ],
    )
    calendar_df.to_csv(CALENDAR_FILE, index=False)
    return start_date, end_date


def write_feed_info_file(start_date, end_date):
    today = date.today().strftime("%Y%m%d")
    feed_info_df = pd.DataFrame(
        [
            {
                "feed_publisher_name": "Indian Railways",
                "feed_publisher_url": "https://www.indianrailways.gov.in/",
                "feed_lang": "en",
                "feed_contact_url": "https://www.indianrailways.gov.in/",
                "feed_start_date": start_date,
                "feed_end_date": end_date,
                "feed_version": today,
            }
        ],
        columns=[
            "feed_publisher_name",
            "feed_publisher_url",
            "feed_lang",
            "feed_contact_url",
            "feed_start_date",
            "feed_end_date",
            "feed_version",
        ],
    )
    feed_info_df.to_csv(FEED_INFO_FILE, index=False)


def parse_coord_value(value):
    if pd.isna(value):
        return None
    text = str(value).strip()
    if not text:
        return None
    return float(text)


def extrapolate_coordinate(sequence, coords_by_stop, anchor_idx, target_idx, step_idx):
    anchor_id = sequence[anchor_idx]
    target_id = sequence[target_idx]
    anchor = coords_by_stop[anchor_id]
    target = coords_by_stop[target_id]
    fraction = (step_idx - anchor_idx) / (target_idx - anchor_idx)
    lat = anchor[0] + (target[0] - anchor[0]) * fraction
    lon = anchor[1] + (target[1] - anchor[1]) * fraction
    return round(lat, 6), round(lon, 6)


def build_inferred_coords(existing_records):
    coords_by_stop = {}

    for stop_id, record in existing_records.items():
        lat = parse_coord_value(record.get("stop_lat", ""))
        lon = parse_coord_value(record.get("stop_lon", ""))
        if lat is not None and lon is not None:
            coords_by_stop[stop_id] = (lat, lon)

    for stop_id, coord in STOP_COORDS.items():
        coords_by_stop[stop_id] = coord

    changed = True
    while changed:
        changed = False

        for sequence in STOP_INTERPOLATION_SEQUENCES:
            for idx, stop_id in enumerate(sequence):
                if stop_id in coords_by_stop:
                    continue

                prev_idx = next((i for i in range(idx - 1, -1, -1) if sequence[i] in coords_by_stop), None)
                next_idx = next((i for i in range(idx + 1, len(sequence)) if sequence[i] in coords_by_stop), None)
                coord = None

                if prev_idx is not None and next_idx is not None:
                    coord = extrapolate_coordinate(sequence, coords_by_stop, prev_idx, next_idx, idx)
                elif prev_idx is not None:
                    prev2_idx = next((i for i in range(prev_idx - 1, -1, -1) if sequence[i] in coords_by_stop), None)
                    if prev2_idx is not None:
                        coord = extrapolate_coordinate(sequence, coords_by_stop, prev2_idx, prev_idx, idx)
                elif next_idx is not None:
                    next2_idx = next((i for i in range(next_idx + 1, len(sequence)) if sequence[i] in coords_by_stop), None)
                    if next2_idx is not None:
                        coord = extrapolate_coordinate(sequence, coords_by_stop, next_idx, next2_idx, idx)

                if coord is not None:
                    coords_by_stop[stop_id] = coord
                    changed = True

    return coords_by_stop


def build_stops_file(existing_stops_df, used_stop_ids):
    existing_stops_df = existing_stops_df.copy()
    existing_records = {}
    for _, row in existing_stops_df.iterrows():
        stop_id = clean_value(row.get("stop_id", ""))
        if stop_id:
            existing_records[stop_id] = row.to_dict()

    inferred_coords = build_inferred_coords(existing_records)

    parent_stop_ids = set()
    for stop_id in used_stop_ids:
        record = existing_records.get(stop_id, {})
        parent_station = clean_value(record.get("parent_station", ""))
        if parent_station:
            parent_stop_ids.add(parent_station)

    final_stop_ids = sorted(used_stop_ids | parent_stop_ids)
    final_rows = []

    for stop_id in final_stop_ids:
        existing = existing_records.get(stop_id, {})
        fallback_name = stop_id.replace("_", " ").title()
        stop_name = (
            CANONICAL_STOP_NAMES.get(stop_id)
            or clean_value(existing.get("stop_name", ""))
            or stop_name_by_id.get(stop_id)
            or fallback_name
        )
        stop_lat = clean_value(existing.get("stop_lat", ""))
        stop_lon = clean_value(existing.get("stop_lon", ""))
        coord = inferred_coords.get(stop_id)
        if coord is not None:
            coord_lat, coord_lon = coord
            if stop_id in STOP_COORDS:
                stop_lat = coord_lat
                stop_lon = coord_lon
            else:
                stop_lat = stop_lat or coord_lat
                stop_lon = stop_lon or coord_lon

        final_rows.append(
            {
                "stop_id": stop_id,
                "stop_name": stop_name,
                "stop_lat": stop_lat,
                "stop_lon": stop_lon,
                "location_type": clean_value(existing.get("location_type", "")) or "0",
                "parent_station": clean_value(existing.get("parent_station", "")),
                "zone_id": clean_value(existing.get("zone_id", "")),
            }
        )

    stops_df = pd.DataFrame(
        final_rows,
        columns=[
            "stop_id",
            "stop_name",
            "stop_lat",
            "stop_lon",
            "location_type",
            "parent_station",
            "zone_id",
        ],
    )
    stops_df.to_csv(STOPS_FILE, index=False)
    return stops_df


def write_gtfs_zip():
    feed_files = [
        "agency.txt",
        "calendar.txt",
        "feed_info.txt",
        "routes.txt",
        "stop_times.txt",
        "stops.txt",
        "trips.txt",
    ]
    if INCLUDE_SHAPES:
        feed_files.append("shapes.txt")
    with zipfile.ZipFile(GTFS_ZIP, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for filename in feed_files:
            path = os.path.join(GTFS_DIR, filename)
            if os.path.exists(path):
                archive.write(path, arcname=filename)


def main():
    os.makedirs(GTFS_DIR, exist_ok=True)

    stops_df = load_existing_stops()
    stop_lookup = {
        normalize_station(row["stop_name"]): row["stop_id"]
        for _, row in stops_df.iterrows()
        if str(row.get("stop_id", "")).strip()
    }
    used_route_ids = set()

    print(f"Loaded {len(stop_lookup)} stops from stops.txt")

    for file in sorted(os.listdir(INPUT_DIR)):
        if not file.endswith(".csv") or file.startswith("._"):
            continue

        route_id = FILE_TO_ROUTE.get(file)
        direction_id = FILE_TO_DIRECTION.get(file, 0)
        trip_prefix = FILE_TO_PREFIX.get(file, file.replace(".csv", ""))

        if route_id is None:
            print(f"  SKIP unknown file: {file}")
            continue

        filepath = os.path.join(INPUT_DIR, file)
        print(f"\nReading: {filepath} (route={route_id}, direction={direction_id})")

        df = load_csv(filepath)
        if file.startswith("CR_timetable_"):
            df = normalize_central_dataframe(file, df)
        layout = detect_layout(df)

        if layout == "train_rows":
            process_train_rows(df, route_id, direction_id, trip_prefix, stop_lookup)
        elif layout == "station_rows":
            process_station_rows(df, route_id, direction_id, trip_prefix, stop_lookup)
        else:
            print(f"  WARNING: Could not detect CSV layout for {file}")
            continue

        used_route_ids.add(route_id)

    stop_df = pd.DataFrame(
        stop_rows,
        columns=["trip_id", "arrival_time", "departure_time", "stop_id", "stop_sequence"],
    )
    trip_df = pd.DataFrame(
        trip_rows,
        columns=["route_id", "service_id", "trip_id", "direction_id", "shape_id"],
    )

    if not stop_df.empty:
        valid_trips = stop_df.groupby("trip_id").filter(lambda rows: len(rows) >= 2)["trip_id"].unique()
        stop_df = stop_df[stop_df["trip_id"].isin(valid_trips)]
        trip_df = trip_df[trip_df["trip_id"].isin(valid_trips)]

    stop_df = stop_df.drop_duplicates(subset=["trip_id", "stop_sequence"])
    trip_df = trip_df.drop_duplicates(subset=["trip_id"])
    stop_df = stop_df.sort_values(["trip_id", "stop_sequence"]) if not stop_df.empty else stop_df

    stop_df.to_csv(STOP_TIMES, index=False)
    trip_df.to_csv(TRIPS, index=False)

    used_stop_ids = set(stop_df.get("stop_id", pd.Series(dtype=str)).astype(str))
    stops_df = build_stops_file(stops_df, used_stop_ids)
    write_routes_file(used_route_ids)
    start_date, end_date = write_calendar_file(set(trip_df.get("service_id", pd.Series(dtype=str)).astype(str)))
    write_feed_info_file(start_date, end_date)
    write_gtfs_zip()

    print("\nGTFS generated successfully")
    print(f"  stop_times: {len(stop_df)} rows")
    print(f"  trips: {len(trip_df)} rows")
    print(f"  stops: {len(stops_df)} rows")
    print(f"  routes: {len(used_route_ids)} rows")
    print(f"  feed zip: {GTFS_ZIP}")


if __name__ == "__main__":
    main()
