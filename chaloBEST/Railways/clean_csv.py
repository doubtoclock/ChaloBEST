import pandas as pd
import glob
import os
import re

INPUT = "parsed/csv"
OUTPUT = "parsed/clean"

os.makedirs(OUTPUT, exist_ok=True)

time_pattern = re.compile(r"^\d{1,2}:\d{2}$")
train_pattern = re.compile(r"^\d{4,6}$")  # train numbers are typically 4-6 digits

files = glob.glob(f"{INPUT}/*.csv")

for file in files:

    name = os.path.basename(file)
    print("Processing:", name)

    df = pd.read_csv(file, header=None, encoding="latin1")

    # Dynamically find the header row containing train numbers
    header_row = None
    for idx in range(min(20, len(df))):  # search first 20 rows
        row = df.iloc[idx].astype(str)
        # Count how many cells look like train numbers
        train_count = sum(1 for val in row if train_pattern.match(str(val).strip()))
        if train_count >= 2:  # at least 2 train numbers found
            header_row = idx
            break

    if header_row is None:
        print(f"  WARNING: Could not find header row in {name}, skipping")
        continue

    print(f"  Found header at row {header_row}")

    # Find the first data row (row after header that has station names)
    data_start = header_row + 1
    # Skip any sub-header rows (e.g., "Arr/Dep" rows)
    for idx in range(header_row + 1, min(header_row + 5, len(df))):
        row_vals = df.iloc[idx].astype(str)
        # If first cell looks like a station name (not empty, not a number pattern)
        first_val = str(row_vals.iloc[0]).strip()
        if first_val and first_val.lower() not in ['nan', '', 'arr', 'dep', 'arr/dep']:
            data_start = idx
            break

    header = df.iloc[header_row]
    data = df.iloc[data_start:].copy()
    data.columns = header

    # Rename first column to "station"
    cols = list(data.columns)
    cols[0] = "station"
    data.columns = cols

    # Remove rows without station
    data = data.dropna(subset=["station"])
    data = data[data["station"].astype(str).str.strip() != ""]
    data = data[data["station"].astype(str).str.lower() != "nan"]

    # Clean time cells - keep multi-time cells (newline separated) intact
    for col in data.columns[1:]:
        def clean_cell(x):
            if not isinstance(x, str):
                return None
            x = x.strip()
            if not x:
                return None
            # Handle newline-separated multiple times
            parts = x.split("\n")
            valid_parts = []
            for p in parts:
                p = p.strip()
                if re.match(r"^\d{1,2}:\d{2}$", p):
                    # Normalize to HH:MM
                    h, m = p.split(":")
                    valid_parts.append(f"{int(h):02d}:{m}")
                elif re.match(r"^\d{1,2}\.\d{2}$", p):
                    # Handle dot notation like 8.30
                    h, m = p.split(".")
                    valid_parts.append(f"{int(h):02d}:{m}")
            if valid_parts:
                return "\n".join(valid_parts)
            return None

        data[col] = data[col].apply(clean_cell)

    data.to_csv(f"{OUTPUT}/{name}", index=False)
    print(f"  Cleaned: {name} ({len(data)} rows, {len(data.columns)-1} trains)")