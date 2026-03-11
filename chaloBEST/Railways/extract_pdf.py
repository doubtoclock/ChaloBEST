import camelot
import pandas as pd
import glob
import os

RAW = "raw"
OUT = "parsed/csv"

os.makedirs(OUT, exist_ok=True)

pdfs = glob.glob(f"{RAW}/*.pdf")

for pdf in pdfs:

    name = os.path.basename(pdf).replace(".pdf", "")
    print("Processing:", name)

    tables = camelot.read_pdf(pdf, pages="all", flavor="stream")

    if len(tables) == 0:
        print(f"  WARNING: No tables found in {name}")
        continue

    # Save each table as a separate CSV for inspection
    for i, table in enumerate(tables):
        debug_file = f"{OUT}/{name}_table_{i}.csv"
        table.df.to_csv(debug_file, index=False, header=False)

    # Also save the combined version
    combined = []
    for table in tables:
        combined.append(table.df)

    final = pd.concat(combined, ignore_index=True)
    out_file = f"{OUT}/{name}.csv"
    final.to_csv(out_file, index=False, header=False)

    print(f"  Saved: {out_file} ({len(tables)} tables, {len(final)} rows)")