import csv
import datetime
from mumbaitrains.models import Train, Station, TrainStation


def parse_csv(file_path, line):
    """
    Parse a timetable CSV and populate Train / Station / TrainStation tables.

    CSV format expected:

    train_no,Churchgate,Marine Lines,Charni Road,...
    90001,06:10,06:12,06:14,...
    """

    with open(file_path, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)

        header = next(reader)
        stations = header[1:]   # first column = train number

        for row in reader:

            train_no = row[0].strip()
            times = row[1:]

            if not train_no:
                continue

            print(f"Saving train {train_no}...")

            train, created = Train.objects.get_or_create(
                number=train_no,
                line=line
            )

            if not created:
                print(f"Train {train_no} already exists, skipping")
                continue

            serial = 0

            for station_name, time_str in zip(stations, times):

                if not time_str.strip():
                    continue

                station, _ = Station.objects.get_or_create(
                    name=station_name.strip()
                )

                try:
                    hour, minute = map(int, time_str.split(":"))
                except ValueError:
                    continue

                station_time = datetime.time(hour, minute)

                TrainStation.objects.create(
                    train=train,
                    station=station,
                    time=station_time,
                    serial=serial
                )

                serial += 1

            print(f"Saved train {train_no}")


if __name__ == "__main__":
    # example usage
    parse_csv("Railways/parsed/csv/Western_up.csv", "WR")