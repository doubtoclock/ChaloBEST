import camelot
import pandas as pd
import glob
import os
import pdfplumber
import re

RAW = "raw"
OUT = "parsed/csv"

os.makedirs(OUT, exist_ok=True)

pdfs = glob.glob(f"{RAW}/*.pdf")

DEBUG = False


# -----------------------------
# Extract train numbers (backup)
# -----------------------------
def extract_train_numbers(pdf):
    numbers = []

    with pdfplumber.open(pdf) as doc:
        for page in doc.pages:
            text = page.extract_text() or ""
            matches = re.findall(r"\b9\d{4}\b", text)

            for m in matches:
                if m not in numbers:
                    numbers.append(m)

    return numbers


# -----------------------------
# Normalize columns
# -----------------------------
def normalize_columns(df):
    df = df.dropna(axis=1, how="all")
    df = df.loc[:, ~(df == "").all()]
    df.columns = range(df.shape[1])
    return df


# -----------------------------
# Extract headers (handles 90007A)
# -----------------------------
def extract_headers(df):
    header_rows = df.iloc[:3]
    headers = []

    for col in range(1, df.shape[1]):
        combined = ""

        for row in header_rows.values:
            val = str(row[col]).strip()
            if val:
                combined += val

        combined = combined.replace(" ", "")

        if re.match(r"^\d{4,6}[A-Z]?$", combined):
            headers.append(combined)
        else:
            headers.append(f"UNK_{col}")

    return ["STATION"] + headers


# -----------------------------
# Clean rows
# -----------------------------
def clean_rows(df):
    df = df.copy()

    df = df[~df.apply(lambda r: all(str(x).strip() == "" for x in r), axis=1)]

    df = df[~df.iloc[:, 0].str.contains(
        "W.E.F|STATION|TRAIN|CAR|Air|Condition|NOT",
        case=False,
        na=False
    )]

    return df.reset_index(drop=True)


# -----------------------------
# Clean time
# -----------------------------
def clean_time(cell):
    cell = str(cell).strip()

    if re.match(r"^\d{2}:\d{2}$", cell):
        return cell

    return ""

# -----------------------------
#fix invalid station names
# ------------------------------
def make_unique(cols):
    seen = {}
    new_cols = []

    for col in cols:
        if col not in seen:
            seen[col] = 0
            new_cols.append(col)
        else:
            seen[col] += 1
            new_cols.append(f"{col}_{seen[col]}")

    return new_cols

# -----------------------------
# MAIN LOOP
# -----------------------------
for pdf in pdfs:

    name = os.path.basename(pdf).replace(".pdf", "")
    print("Processing:", name)

    train_numbers = extract_train_numbers(pdf)

    tables = camelot.read_pdf(
        pdf,
        pages="all",
        flavor="stream",
        split_text=True,
        strip_text="\n",
        edge_tol=500,
        row_tol=10
    )

    if len(tables) == 0:
        print(f"  WARNING: No tables found in {name}")
        continue

    combined = []

    for table in tables:

        df = table.df

        # STEP 1: normalize
        df = normalize_columns(df)

        if df.shape[1] < 2:
            continue

        # STEP 2: headers
        headers = extract_headers(df)

        if all("UNK" in h for h in headers[1:]) and len(train_numbers) >= df.shape[1] - 1:
            headers = ["STATION"] + train_numbers[: df.shape[1] - 1]

        df.columns = headers

        # STEP 3: remove header rows
        df = df.iloc[3:]

        # STEP 4: clean station column
        df.iloc[:, 0] = df.iloc[:, 0].astype(str).str.strip()

        # STEP 5: clean rows
        df = clean_rows(df)

        # STEP 6: clean times
        for col in df.columns[1:]:
            df[col] = df[col].apply(clean_time)

        # -----------------------------
        # 🔥 KEY IDEA: TRANSPOSE
        # -----------------------------
        df = df.set_index("STATION")
        df = df.transpose()
        df.columns = make_unique(df.columns)

        df.reset_index(inplace=True)
        df.rename(columns={"index": "TRAIN"}, inplace=True)

        # DEBUG
        if DEBUG:
            debug_file = f"{OUT}/{name}_table_{len(combined)}.csv"
            df.to_csv(debug_file, index=False)

        combined.append(df)

    if not combined:
        print(f"  WARNING: No usable tables in {name}")
        continue

    # 🔥 simple vertical join now
    final = pd.concat(combined, ignore_index=True)

    out_file = f"{OUT}/{name}.csv"
    final.to_csv(out_file, index=False)

    print(f"  Saved: {out_file} ({len(final)} trains)")


print("\n✅ PDF parsing complete")