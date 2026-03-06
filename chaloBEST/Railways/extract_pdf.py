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

    combined = []

    for table in tables:
        df = table.df
        combined.append(df)

    final = pd.concat(combined, ignore_index=True)

    out_file = f"{OUT}/{name}.csv"
    final.to_csv(out_file, index=False)

    print("Saved:", out_file)