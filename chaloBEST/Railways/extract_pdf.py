import glob
import os
import re
from functools import lru_cache

import camelot # type: ignore
import pandas as pd # type: ignore

from generate_GTFS import ALIAS_TO_STOP_ID, CANONICAL_STOP_NAMES

RAW = "raw"
OUT = "parsed/csv"
DEBUG = False

TIME_PATTERN = re.compile(r"\d{1,2}:\d{2}")
TRAIN_PATTERN = re.compile(r"^\d{4,6}[A-Z]?$")
ANNOTATION_PATTERN = re.compile(
    r"W\.?E\.?F|STATIONS?|TRAINS?|UP TRAINS|DN TRAINS|CAR|AIR|CONDITION|"
    r"NOT|ONLY|SUN|SAT|LADIES|SPL|GEN|AC",
    re.IGNORECASE,
)

STATION_ALIASES = {
    "CSMT": "CSMT",
    "MUMBAI CSMT": "Mumbai CSMT",
    "CHHATRAPATI SHIVAJI MAHARAJ TERMINUS": "Mumbai CSMT",
    "MASJID": "Masjid",
    "SANDHURST ROAD": "Sandhurst Road",
    "BYCULLA": "Byculla",
    "CHINCHPOKLI": "Chinchpokli",
    "CURREY ROAD": "Currey Road",
    "PAREL": "Parel",
    "MATUNGA": "Matunga",
    "SION": "Sion",
    "KURLA": "Kurla",
    "VIDYAVIHAR": "Vidyavihar",
    "GHATKOPAR": "Ghatkopar",
    "VIKHROLI": "Vikhroli",
    "KANJUR MARG": "Kanjur Marg",
    "KANJURMARG": "Kanjur Marg",
    "BHANDUP": "Bhandup",
    "NAHUR": "Nahur",
    "MULUND": "Mulund",
    "THANE": "Thane",
    "KALWA": "Kalwa",
    "KALVA": "Kalwa",
    "MUMBRA": "Mumbra",
    "DIVA": "Diva Junction",
    "DIWA": "Diva Junction",
    "DIVA JUNCTION": "Diva Junction",
    "KOPAR": "Kopar",
    "KALYAN": "Kalyan",
    "KALYAN JUNCTION": "Kalyan",
    "VITHALWADI": "Vithalwadi",
    "ULHASNAGAR": "Ulhasnagar",
    "AMBERNATH": "Ambernath",
    "BADLAPUR": "Badlapur",
    "VANGANI": "Vangani",
    "SHELU": "Shelu",
    "NERAL": "Neral",
    "BHIVPURI ROAD": "Bhivpuri Road",
    "KARJAT": "Karjat",
    "PALASDHARI": "Palasdhari",
    "KASARA": "Kasara",
    "SHAHAD": "Shahad",
    "AMBIVLI": "Ambivli",
    "TITWALA": "Titwala",
    "KHADAVLI": "Khadavli",
    "VASIND": "Vasind",
    "ASANGAON": "Asangaon",
    "ATGAON": "Atgaon",
    "THANSIT": "Thansit",
    "KHARDI": "Khardi",
    "UMBERMALLI": "Umbermali",
    "UMBERMALI": "Umbermali",
    "KELAVLI": "Kelavli",
    "DOLAVLI": "Dolavli",
    "LOWJEE": "Lowjee",
    "KHOPOLI": "Khopoli",
    "CHURCHGATE": "Churchgate",
    "MARINE LINES": "Marine Lines",
    "CHARNI ROAD": "Charni Road",
    "GRANT ROAD": "Grant Road",
    "M BAI CENTRAL": "Mumbai Central",
    "MBAI CENTRAL": "Mumbai Central",
    "MUMBAI CENTRAL": "Mumbai Central",
    "MAHALAKSHMI": "Mahalakshmi",
    "LOWER PAREL": "Lower Parel",
    "PRABHADEVI": "Prabhadevi",
    "DADAR": "Dadar",
    "MATUNGA ROAD": "Matunga Road",
    "MAHIM JN": "Mahim Jn",
    "MAHIM JUNCTION": "Mahim Jn",
    "BANDRA": "Bandra",
    "KHAR ROAD": "Khar Road",
    "SANTA CRUZ": "Santa Cruz",
    "VILE PARLE": "Vile Parle",
    "ANDHERI": "Andheri",
    "JOGESHWARI": "Jogeshwari",
    "RAM MANDIR": "Ram Mandir",
    "GOREGAON": "Goregaon",
    "MALAD": "Malad",
    "KANDIVLI": "Kandivali",
    "KANDIVALI": "Kandivali",
    "BORIVALI": "Borivali",
    "DAHISAR": "Dahisar",
    "MIRA ROAD": "Mira Road",
    "BHAYANDAR": "Bhayandar",
    "NAIGAON": "Naigaon",
    "VASAI ROAD": "Vasai Road",
    "NALLA SOPARA": "Nalla Sopara",
    "NALLASOPARA": "Nalla Sopara",
    "VIRAR": "Virar",
    "DOCKYARD ROAD": "Dockyard Road",
    "REAY ROAD": "Reay Road",
    "COTTON GREEN": "Cotton Green",
    "SEWRI": "Sewri",
    "VADALA ROAD": "Vadala Road",
    "GTB NAGAR": "GTB Nagar",
    "CHUNABHATTI": "Chunabhatti",
    "TILAK NAGAR": "Tilak Nagar",
    "TILAKNAGAR": "Tilak Nagar",
    "CHEMBUR": "Chembur",
    "GOVANDI": "Govandi",
    "MANKHURD": "Mankhurd",
    "VASHI": "Vashi",
    "SANPADA": "Sanpada",
    "JUINAGAR": "Juinagar",
    "NERUL": "Nerul",
    "SEAWOOD DARAVE": "Seawoods Darave",
    "SEAWOODS DARAVE": "Seawoods Darave",
    "BELAPUR CBD": "CBD Belapur",
    "CBD BELAPUR": "CBD Belapur",
    "KHARGHAR": "Kharghar",
    "KHANDESHWAR": "Khandeshwar",
    "MANSAROVAR": "Mansarovar",
    "PANVEL": "Panvel",
    "KINGS CIRCLE": "King's Circle",
}

ELLIPSIS_PATTERN = re.compile(r"^[^0-9]*$|^[^0-9]+|[^0-9]+$")
STATION_TOKEN_OVERRIDES = {
    "CSMT": "CSMT",
    "MUMBAICSMT": "Mumbai CSMT",
}


def format_station_label(key):
    words = key.split()
    fixed = []
    for word in words:
        if word in {"CSMT", "GTB", "CBD"}:
            fixed.append(word)
        elif word == "JN":
            fixed.append("Jn")
        else:
            fixed.append(word.capitalize())
    return " ".join(fixed)


def normalize_columns(df):
    df = df.dropna(axis=1, how="all").copy()
    df = df.loc[:, ~(df == "").all()]
    df.columns = range(df.shape[1])
    return df


def normalize_station_key(value):
    text = str(value).replace("\n", " ").strip().upper()
    text = text.replace("'", " ")
    text = text.replace(".", " ")
    text = text.replace("(", " ")
    text = text.replace(")", " ")
    text = text.replace("-", " ")
    text = " ".join(text.split())
    return text


STOP_ID_TO_NAME = {}
for alias, stop_id in ALIAS_TO_STOP_ID.items():
    STOP_ID_TO_NAME.setdefault(stop_id, format_station_label(alias))
STOP_ID_TO_NAME.update(CANONICAL_STOP_NAMES)

KNOWN_STATION_NAMES = sorted(
    {
        *STATION_ALIASES.values(),
        *STOP_ID_TO_NAME.values(),
    },
    key=lambda name: (-len(normalize_station_key(name).replace(" ", "")), name),
)
KNOWN_STATION_TOKENS = {}
for alias, station in STATION_ALIASES.items():
    KNOWN_STATION_TOKENS[alias.replace(" ", "")] = station

for alias, stop_id in ALIAS_TO_STOP_ID.items():
    KNOWN_STATION_TOKENS[alias.replace(" ", "")] = STOP_ID_TO_NAME.get(stop_id, format_station_label(alias))

for name in KNOWN_STATION_NAMES:
    KNOWN_STATION_TOKENS[normalize_station_key(name).replace(" ", "")] = name

KNOWN_STATION_TOKEN_ITEMS = sorted(
    KNOWN_STATION_TOKENS.items(),
    key=lambda item: (-len(item[0]), item[0]),
)


def canonical_station_name(value):
    text = str(value).replace("\n", " ").strip()
    if not text:
        return ""

    key = normalize_station_key(text)
    if not key or not re.search(r"[A-Z]", key):
        return ""

    if "CENTRAL" in key and any(token in key for token in ("MBAI", "M BAI", "MUMBAI")):
        return "Mumbai Central"

    stop_id = ALIAS_TO_STOP_ID.get(key)
    if stop_id:
        return STOP_ID_TO_NAME.get(stop_id, format_station_label(key))

    return STATION_ALIASES.get(key, text)


def extract_times(cell):
    return TIME_PATTERN.findall(str(cell))


def extract_time(cell):
    times = extract_times(cell)
    return times[0] if times else ""


def compact_station_text(value):
    key = normalize_station_key(value)
    key = re.sub(r"\d+", "", key)
    return STATION_TOKEN_OVERRIDES.get(key, key.replace(" ", ""))


@lru_cache(maxsize=None)
def split_station_label(label):
    compact = compact_station_text(label)
    if not compact:
        return ()

    if compact in KNOWN_STATION_TOKENS:
        return (KNOWN_STATION_TOKENS[compact],)

    @lru_cache(maxsize=None)
    def backtrack(start):
        if start == len(compact):
            return ()

        best = None
        for token, station in KNOWN_STATION_TOKEN_ITEMS:
            if compact.startswith(token, start):
                remainder = backtrack(start + len(token))
                if remainder is None:
                    continue

                candidate = (station, *remainder)
                if best is None or len(candidate) < len(best):
                    best = candidate

        return best

    result = backtrack(0)
    if result:
        return result

    station = canonical_station_name(label)
    return (station,) if station else ()


def distribute_cell_times(cell, station_count):
    values = [""] * station_count
    times = extract_times(cell)
    if not times or station_count == 0:
        return values

    if len(times) >= station_count:
        return times[:station_count]

    if len(times) == 1 and station_count == 2:
        token = times[0]
        raw = re.sub(r"\s+", "", str(cell))
        before, _, after = raw.partition(token)
        before_gap = bool(before) and ELLIPSIS_PATTERN.match(before)
        after_gap = bool(after) and ELLIPSIS_PATTERN.match(after)

        if before_gap and not after_gap:
            values[-1] = token
            return values

        if after_gap and not before_gap:
            values[0] = token
            return values

    for idx, token in enumerate(times):
        values[idx] = token

    return values


def find_train_row(df):
    best_idx = None
    best_score = -1

    for idx in range(min(6, len(df))):
        score = 0
        for col in range(1, df.shape[1]):
            token = re.sub(r"\s+", "", str(df.iat[idx, col])).upper()
            if TRAIN_PATTERN.match(token):
                score += 1

        if score > best_score:
            best_idx = idx
            best_score = score

    return best_idx if best_score > 0 else None


def extract_headers(df):
    train_row_idx = find_train_row(df)
    if train_row_idx is None:
        return None, None

    headers = []
    for col in range(1, df.shape[1]):
        token = re.sub(r"\s+", "", str(df.iat[train_row_idx, col])).upper()
        headers.append(token if TRAIN_PATTERN.match(token) else f"UNK_{col}")

    return ["STATION"] + headers, train_row_idx


def find_data_start(df, train_row_idx):
    for idx in range(train_row_idx + 1, len(df)):
        station = canonical_station_name(df.iat[idx, 0])
        has_time = any(extract_time(df.iat[idx, col]) for col in range(1, df.shape[1]))
        if station and has_time:
            return idx

    return train_row_idx + 1


def clean_rows(df):
    cleaned = []

    for _, row in df.iterrows():
        row = row.copy()
        raw_station = str(row.iloc[0]).strip()
        row_text = " ".join(str(cell).strip() for cell in row if str(cell).strip())

        if ANNOTATION_PATTERN.search(row_text) and not any(
            extract_times(cell) for cell in row.iloc[1:]
        ):
            continue

        stations = split_station_label(raw_station)
        if not stations:
            continue

        if len(stations) == 1:
            row.iloc[0] = stations[0]
            cleaned.append(row)
            continue

        split_values = [distribute_cell_times(cell, len(stations)) for cell in row.iloc[1:]]
        for station_idx, station in enumerate(stations):
            expanded = row.copy()
            expanded.iloc[0] = station
            for col_idx, values in enumerate(split_values, start=1):
                expanded.iloc[col_idx] = values[station_idx]
            cleaned.append(expanded)

    if not cleaned:
        return pd.DataFrame(columns=df.columns)

    return pd.DataFrame(cleaned, columns=df.columns).reset_index(drop=True)


def collapse_duplicate_columns(df):
    collapsed = pd.DataFrame(index=df.index)

    for idx, name in enumerate(df.columns):
        column_name = str(name).strip()
        if not column_name:
            continue

        values = df.iloc[:, idx].fillna("")
        if column_name not in collapsed.columns:
            collapsed[column_name] = values
            continue

        existing = collapsed[column_name].fillna("").astype(str)
        incoming = values.astype(str)
        fill_mask = existing.str.strip().eq("") & incoming.str.strip().ne("")
        collapsed.loc[fill_mask, column_name] = incoming[fill_mask]

    return collapsed


def is_blank(value):
    return pd.isna(value) or str(value).strip() == ""


def canonicalize_output_column(name):
    label = str(name).strip()
    if label == "TRAIN":
        return label

    station = canonical_station_name(label)
    return station or label


def collapse_duplicate_trains(df):
    merged_rows = {}
    train_order = []

    for _, row in df.iterrows():
        train = str(row["TRAIN"]).strip()
        if train not in merged_rows:
            merged_rows[train] = row.copy()
            train_order.append(train)
            continue

        existing = merged_rows[train]
        for col in df.columns[1:]:
            if is_blank(existing[col]) and not is_blank(row[col]):
                existing[col] = row[col]

    return pd.DataFrame([merged_rows[train] for train in train_order], columns=df.columns)


def finalize_output(df):
    df = df.rename(columns=lambda col: canonicalize_output_column(col))

    ordered_columns = []
    merged_columns = {}

    for idx, col in enumerate(df.columns):
        series = df.iloc[:, idx]
        if col not in merged_columns:
            merged_columns[col] = series.copy()
            ordered_columns.append(col)
            continue

        existing = merged_columns[col]
        fill_mask = existing.apply(is_blank) & ~series.apply(is_blank)
        existing.loc[fill_mask] = series.loc[fill_mask]
        merged_columns[col] = existing

    finalized = pd.DataFrame({col: merged_columns[col] for col in ordered_columns})
    finalized = collapse_duplicate_trains(finalized)
    return finalized


def main():
    os.makedirs(OUT, exist_ok=True)
    pdfs = glob.glob(f"{RAW}/*.pdf")

    for pdf in pdfs:
        name = os.path.basename(pdf).replace(".pdf", "")
        print("Processing:", name)

        tables = camelot.read_pdf(
            pdf,
            pages="all",
            flavor="stream",
            split_text=True,
            strip_text="\n",
            edge_tol=500,
            row_tol=10,
        )

        if len(tables) == 0:
            print(f"  WARNING: No tables found in {name}")
            continue

        combined = []

        for table_idx, table in enumerate(tables):
            df = normalize_columns(table.df)
            if df.shape[1] < 2:
                continue

            headers, train_row_idx = extract_headers(df)
            if headers is None:
                continue

            df.columns = headers
            df = df.iloc[find_data_start(df, train_row_idx):].reset_index(drop=True)
            df = clean_rows(df)

            if df.empty:
                continue

            for col in df.columns[1:]:
                df[col] = df[col].apply(extract_time)

            time_columns = [
                col
                for col in df.columns[1:]
                if not str(col).startswith("UNK_") and df[col].astype(str).str.strip().ne("").any()
            ]

            if not time_columns:
                continue

            df = df[["STATION"] + time_columns]
            transposed = df.set_index("STATION").transpose()
            transposed = collapse_duplicate_columns(transposed)
            transposed = transposed.loc[:, transposed.astype(str).apply(
                lambda col: col.str.strip().ne("").any(), axis=0
            )]
            transposed.reset_index(inplace=True)
            transposed.rename(columns={"index": "TRAIN"}, inplace=True)

            if DEBUG:
                debug_file = f"{OUT}/{name}_table_{table_idx}.csv"
                transposed.to_csv(debug_file, index=False)

            combined.append(transposed)

        if not combined:
            print(f"  WARNING: No usable tables in {name}")
            continue

        final = pd.concat(combined, ignore_index=True)
        final = final[final["TRAIN"].astype(str).str.match(TRAIN_PATTERN)]
        final = finalize_output(final)
        final.to_csv(f"{OUT}/{name}.csv", index=False)

        print(f"  Saved: {OUT}/{name}.csv ({len(final)} trains)")

    print("\n✅ PDF parsing complete")


if __name__ == "__main__":
    main()
