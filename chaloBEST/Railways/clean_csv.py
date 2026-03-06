import pandas as pd
import glob
import os
import re

INPUT = "parsed/csv"
OUTPUT = "parsed/clean"

os.makedirs(OUTPUT, exist_ok=True)

time_pattern = re.compile(r"\d{2}:\d{2}")

files = glob.glob(f"{INPUT}/*.csv")

for file in files:

    name = os.path.basename(file)

    df = pd.read_csv(file, header=None)

    # train numbers row
    header = df.iloc[3]

    # timetable data
    data = df.iloc[5:].copy()

    data.columns = header

    # rename first column
    data.rename(columns={data.columns[0]: "station"}, inplace=True)

    # remove rows without station
    data = data.dropna(subset=["station"])

    # clean cells
    for col in data.columns[1:]:
        data[col] = data[col].apply(
            lambda x: x if isinstance(x,str) and time_pattern.match(x) else None
        )

    data.to_csv(f"{OUTPUT}/{name}", index=False)

    print("Cleaned:", name)