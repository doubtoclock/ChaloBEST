import pandas as pd
import glob
import os
import re

INPUT = "parsed/csv"
OUTPUT = "parsed/clean"

os.makedirs(OUTPUT, exist_ok=True)

time_pattern = re.compile(r"^\d{1,2}:\d{2}$")
train_pattern = re.compile(r"^\d{4,6}$")


def clean_time(val):
    """Clean a single cell value, return normalized time(s) or None."""
    if not isinstance(val, str):
        return None
    val = val.strip()
    if not val:
        return None

    parts = val.split("\n")
    valid = []
    for p in parts:
        p = p.strip()
        if re.match(r"^\d{1,2}:\d{2}$", p):
            h, m = p.split(":")
            valid.append(f"{int(h):02d}:{m}")
        elif re.match(r"^\d{1,2}\.\d{2}$", p):
            h, m = p.split(".")
            valid.append(f"{int(h):02d}:{m}")
    return "\n".join(valid) if valid else None


def is_station_name(val):
    """Check if a value looks like a station name (not a time, not a number, not empty)."""
    s = str(val).strip()
    if not s or s.lower() == "nan":
        return False
    if re.match(r"^\d{1,2}[:.]\d{2}$", s):
        return False
    if re.match(r"^\d+$", s):
        return False
    if s.startswith("Unnamed"):
        return False
    # Skip known non-station values
    skip = ["arr", "dep", "arr/dep", "x", "$", "condition", "air",
            "stations", "l/spl"]
    if s.lower() in skip:
        return False
    return True


def process_cr_wr(df, name):
    """
    Process Central/Western Railway CSV.
    Format: Repeated sections per PDF page, each with a train-number header row
    and station names in the first column.
    """
    # Find ALL header rows with train numbers
    header_rows = []
    for idx in range(len(df)):
        row = df.iloc[idx]
        train_count = sum(1 for val in row if train_pattern.match(str(val).strip()))
        if train_count >= 2:
            header_rows.append(idx)

    if not header_rows:
        print(f"  WARNING: No train number header found in {name}")
        return None

    print(f"  CR/WR format: found {len(header_rows)} header rows at {header_rows}")

    # Process each section
    all_trains = []
    train_set = set()
    station_times = {}  # station_name -> { train_number -> time }
    station_order = []

    for sec_idx, h_row in enumerate(header_rows):
        # Determine section end
        if sec_idx + 1 < len(header_rows):
            sec_end = header_rows[sec_idx + 1]
        else:
            sec_end = len(df)

        header = df.iloc[h_row]

        # Map col_idx -> train number
        section_trains = []
        for col_idx in range(1, len(header)):
            tn = str(header.iloc[col_idx]).strip()
            if train_pattern.match(tn):
                section_trains.append((col_idx, tn))
                if tn not in train_set:
                    all_trains.append(tn)
                    train_set.add(tn)

        # Data rows
        for row_idx in range(h_row + 1, sec_end):
            station_raw = str(df.iloc[row_idx, 0]).strip()
            if not is_station_name(station_raw):
                continue

            if station_raw not in station_times:
                station_times[station_raw] = {}
                station_order.append(station_raw)

            for col_idx, tn in section_trains:
                cell = str(df.iloc[row_idx, col_idx]).strip()
                time_val = clean_time(cell)
                if time_val:
                    if tn in station_times[station_raw]:
                        existing = station_times[station_raw][tn]
                        if time_val not in existing:
                            station_times[station_raw][tn] = existing + "\n" + time_val
                    else:
                        station_times[station_raw][tn] = time_val

    # Build output
    out_rows = []
    for station in station_order:
        row = {"station": station}
        for tn in all_trains:
            row[tn] = station_times[station].get(tn, None)
        out_rows.append(row)

    result = pd.DataFrame(out_rows)
    print(f"  Merged: {len(station_order)} stations, {len(all_trains)} trains")
    return result


def process_hr(df, name):
    """
    Process Harbour Railway CSV.
    Format: Multiple page sections, each with:
      - A row starting with "Stations" containing train numbers
      - Station names in column 0
      - Times in the grid
      - Repeated sections for each PDF page
    """
    print(f"  HR format detected")

    # Split into sections based on "Stations" rows
    sections = []
    current_train_row = None
    current_data_rows = []

    for idx in range(len(df)):
        first_cell = str(df.iloc[idx, 0]).strip().lower()

        if first_cell == "stations":
            # Save previous section
            if current_train_row is not None and current_data_rows:
                sections.append((current_train_row, current_data_rows))
            current_train_row = idx
            current_data_rows = []
        elif current_train_row is not None:
            # Check if this is a data row (station name in first column)
            raw_first = str(df.iloc[idx, 0]).strip()
            # Skip descriptor rows (service type codes like "GN 2", "PL 2", etc.)
            # Skip condition rows ("X", "$", empty, or rows where first col is empty)
            if is_station_name(raw_first) and not re.match(r"^[A-Z]{1,4}\s+\d+$", raw_first):
                current_data_rows.append(idx)

    # Don't forget last section
    if current_train_row is not None and current_data_rows:
        sections.append((current_train_row, current_data_rows))

    if not sections:
        print(f"  WARNING: No HR sections found in {name}")
        return None

    print(f"  Found {len(sections)} sections")

    # Build unified output: station, train1, train2, ...
    # Each section has different trains, so we build a dict:
    # { station_name -> { train_number -> time } }
    all_trains = []  # ordered list of train numbers
    train_set = set()
    station_times = {}  # station_name -> { train_number -> time }
    station_order = []  # preserve first-seen order

    for train_row_idx, data_row_indices in sections:
        train_row = df.iloc[train_row_idx]

        # Extract train numbers from this section (columns 1+)
        section_trains = []
        for col_idx in range(1, len(train_row)):
            tn = str(train_row.iloc[col_idx]).strip()
            if train_pattern.match(tn):
                section_trains.append((col_idx, tn))
                if tn not in train_set:
                    all_trains.append(tn)
                    train_set.add(tn)

        # Extract data
        for row_idx in data_row_indices:
            station = str(df.iloc[row_idx, 0]).strip()

            if station not in station_times:
                station_times[station] = {}
                station_order.append(station)

            for col_idx, tn in section_trains:
                cell = str(df.iloc[row_idx, col_idx]).strip()
                time_val = clean_time(cell)
                if time_val:
                    # If we already have a time for this station+train, append
                    if tn in station_times[station]:
                        existing = station_times[station][tn]
                        # Don't duplicate
                        if time_val not in existing:
                            station_times[station][tn] = existing + "\n" + time_val
                    else:
                        station_times[station][tn] = time_val

    # Build output DataFrame
    out_rows = []
    for station in station_order:
        row = {"station": station}
        for tn in all_trains:
            row[tn] = station_times[station].get(tn, None)
        out_rows.append(row)

    result = pd.DataFrame(out_rows)
    print(f"  Merged: {len(station_order)} stations, {len(all_trains)} trains")
    return result


# ---- Main ----
# Only process the main combined CSVs (not _table_ debug files)
files = glob.glob(f"{INPUT}/*.csv")
files = [f for f in files if "_table_" not in os.path.basename(f)]

for file in files:
    name = os.path.basename(file)

    # Skip macOS hidden files
    if name.startswith("._"):
        continue

    print(f"\nProcessing: {name}")

    df = pd.read_csv(file, header=None, encoding="latin1")

    # Detect format: does any row start with "Stations"?
    is_hr = False
    for idx in range(len(df)):
        first_cell = str(df.iloc[idx, 0]).strip().lower()
        if first_cell == "stations":
            is_hr = True
            break

    if is_hr:
        result = process_hr(df, name)
    else:
        result = process_cr_wr(df, name)

    if result is not None and len(result) > 0:
        result.to_csv(f"{OUTPUT}/{name}", index=False)
        print(f"  Saved: {name} ({len(result)} rows, {len(result.columns)-1} trains)")
    else:
        print(f"  WARNING: No data extracted from {name}")