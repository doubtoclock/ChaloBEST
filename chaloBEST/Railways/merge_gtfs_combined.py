import csv
import os
import zipfile
from typing import Dict, List, Tuple


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
RAIL_GTFS_DIR = os.path.join(BASE_DIR, "gtfs")
COMBINED_GTFS_DIR = os.path.join(BASE_DIR, "gtfs combined")


def read_csv(path: str) -> Tuple[List[str], List[Dict[str, str]]]:
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        return [], []

    with open(path, newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        rows = []
        for row in reader:
            rows.append({key: (value if value is not None else "") for key, value in row.items()})
        return reader.fieldnames or [], rows


def write_csv(path: str, fieldnames: List[str], rows: List[Dict[str, str]]) -> None:
    with open(path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fieldnames})


def ordered_union(primary: List[str], secondary: List[str]) -> List[str]:
    result = list(primary)
    for item in secondary:
        if item not in result:
            result.append(item)
    return result


def merge_row(existing: Dict[str, str], incoming: Dict[str, str], fieldnames: List[str]) -> Dict[str, str]:
    merged = dict(existing)
    for field in fieldnames:
        if not merged.get(field, "").strip() and incoming.get(field, "").strip():
            merged[field] = incoming[field]
    return merged


def merge_keyed_rows(
    base_rows: List[Dict[str, str]],
    incoming_rows: List[Dict[str, str]],
    fieldnames: List[str],
    key_fields: List[str],
) -> List[Dict[str, str]]:
    merged_rows: List[Dict[str, str]] = []
    key_to_index: Dict[Tuple[str, ...], int] = {}

    for row in base_rows:
        key = tuple(row.get(field, "") for field in key_fields)
        key_to_index[key] = len(merged_rows)
        merged_rows.append({field: row.get(field, "") for field in fieldnames})

    for row in incoming_rows:
        normalized = {field: row.get(field, "") for field in fieldnames}
        key = tuple(normalized.get(field, "") for field in key_fields)
        if key in key_to_index:
            idx = key_to_index[key]
            merged_rows[idx] = merge_row(merged_rows[idx], normalized, fieldnames)
        else:
            key_to_index[key] = len(merged_rows)
            merged_rows.append(normalized)

    return merged_rows


def merge_agency() -> None:
    base_path = os.path.join(COMBINED_GTFS_DIR, "agency.txt")
    incoming_path = os.path.join(RAIL_GTFS_DIR, "agency.txt")
    base_fields, base_rows = read_csv(base_path)
    incoming_fields, incoming_rows = read_csv(incoming_path)
    fieldnames = ordered_union(base_fields, incoming_fields)
    rows = merge_keyed_rows(base_rows, incoming_rows, fieldnames, ["agency_id"])
    write_csv(base_path, fieldnames, rows)


def merge_routes() -> None:
    base_path = os.path.join(COMBINED_GTFS_DIR, "routes.txt")
    incoming_path = os.path.join(RAIL_GTFS_DIR, "routes.txt")
    base_fields, base_rows = read_csv(base_path)
    incoming_fields, incoming_rows = read_csv(incoming_path)

    fieldnames = ordered_union(base_fields, incoming_fields)
    if "route_text_color" not in fieldnames:
        fieldnames.append("route_text_color")

    rows = merge_keyed_rows(base_rows, incoming_rows, fieldnames, ["route_id"])
    write_csv(base_path, fieldnames, rows)


def merge_trips() -> None:
    base_path = os.path.join(COMBINED_GTFS_DIR, "trips.txt")
    incoming_path = os.path.join(RAIL_GTFS_DIR, "trips.txt")
    _, base_rows = read_csv(base_path)
    _, incoming_rows = read_csv(incoming_path)

    fieldnames = ["route_id", "service_id", "trip_id", "trip_headsign", "direction_id"]
    normalized_base = [{field: row.get(field, "") for field in fieldnames} for row in base_rows]
    normalized_incoming = []
    for row in incoming_rows:
        normalized_incoming.append(
            {
                "route_id": row.get("route_id", ""),
                "service_id": row.get("service_id", ""),
                "trip_id": row.get("trip_id", ""),
                "trip_headsign": "",
                "direction_id": row.get("direction_id", ""),
            }
        )

    rows = merge_keyed_rows(normalized_base, normalized_incoming, fieldnames, ["trip_id"])
    write_csv(base_path, fieldnames, rows)


def merge_stops() -> None:
    base_path = os.path.join(COMBINED_GTFS_DIR, "stops.txt")
    incoming_path = os.path.join(RAIL_GTFS_DIR, "stops.txt")
    base_fields, base_rows = read_csv(base_path)
    _, incoming_rows = read_csv(incoming_path)

    fieldnames = list(base_fields)
    normalized_incoming = []
    for row in incoming_rows:
        normalized_incoming.append({field: row.get(field, "") for field in fieldnames})

    rows = merge_keyed_rows(base_rows, normalized_incoming, fieldnames, ["stop_id"])
    write_csv(base_path, fieldnames, rows)


def merge_stop_times() -> None:
    base_path = os.path.join(COMBINED_GTFS_DIR, "stop_times.txt")
    incoming_path = os.path.join(RAIL_GTFS_DIR, "stop_times.txt")
    _, base_rows = read_csv(base_path)
    _, incoming_rows = read_csv(incoming_path)

    fieldnames = ["trip_id", "stop_id", "stop_sequence", "arrival_time", "departure_time"]
    normalized_base = [{field: row.get(field, "") for field in fieldnames} for row in base_rows]
    normalized_incoming = []
    for row in incoming_rows:
        normalized_incoming.append(
            {
                "trip_id": row.get("trip_id", ""),
                "stop_id": row.get("stop_id", ""),
                "stop_sequence": row.get("stop_sequence", ""),
                "arrival_time": row.get("arrival_time", ""),
                "departure_time": row.get("departure_time", ""),
            }
        )

    rows = merge_keyed_rows(
        normalized_base,
        normalized_incoming,
        fieldnames,
        ["trip_id", "stop_sequence"],
    )
    write_csv(base_path, fieldnames, rows)


def merge_calendar() -> None:
    base_path = os.path.join(COMBINED_GTFS_DIR, "calendar.txt")
    incoming_path = os.path.join(RAIL_GTFS_DIR, "calendar.txt")
    base_fields, base_rows = read_csv(base_path)
    incoming_fields, incoming_rows = read_csv(incoming_path)
    fieldnames = ordered_union(base_fields, incoming_fields)
    rows = merge_keyed_rows(base_rows, incoming_rows, fieldnames, ["service_id"])
    _, trip_rows = read_csv(os.path.join(COMBINED_GTFS_DIR, "trips.txt"))
    used_service_ids = {row.get("service_id", "") for row in trip_rows if row.get("service_id", "").strip()}
    rows = [row for row in rows if row.get("service_id", "") in used_service_ids]
    write_csv(base_path, fieldnames, rows)


def write_feed_info() -> None:
    calendar_fields, calendar_rows = read_csv(os.path.join(COMBINED_GTFS_DIR, "calendar.txt"))
    start_dates = [row.get("start_date", "") for row in calendar_rows if row.get("start_date", "").strip()]
    end_dates = [row.get("end_date", "") for row in calendar_rows if row.get("end_date", "").strip()]
    feed_path = os.path.join(COMBINED_GTFS_DIR, "feed_info.txt")

    start_date = min(start_dates) if start_dates else ""
    end_date = max(end_dates) if end_dates else ""
    fieldnames = [
        "feed_publisher_name",
        "feed_publisher_url",
        "feed_lang",
        "feed_contact_url",
        "feed_start_date",
        "feed_end_date",
        "feed_version",
    ]
    rows = [
        {
            "feed_publisher_name": "Mumbai Transit and Railways",
            "feed_publisher_url": "https://mmrda.maharashtra.gov.in/",
            "feed_lang": "en",
            "feed_contact_url": "https://mmrda.maharashtra.gov.in/",
            "feed_start_date": start_date,
            "feed_end_date": end_date,
            "feed_version": "20260424",
        }
    ]
    write_csv(feed_path, fieldnames, rows)


def cleanup_empty_optional_files() -> None:
    for filename in ["calendar_dates.txt"]:
        path = os.path.join(COMBINED_GTFS_DIR, filename)
        if os.path.exists(path) and os.path.getsize(path) == 0:
            os.remove(path)


def write_gtfs_zip() -> None:
    zip_path = os.path.join(COMBINED_GTFS_DIR, "gtfs.zip")
    feed_files = [
        "agency.txt",
        "calendar.txt",
        "calendar_dates.txt",
        "feed_info.txt",
        "frequencies.txt",
        "routes.txt",
        "stop_times.txt",
        "stops.txt",
        "trips.txt",
    ]
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for filename in feed_files:
            path = os.path.join(COMBINED_GTFS_DIR, filename)
            if os.path.exists(path) and os.path.getsize(path) > 0:
                archive.write(path, arcname=filename)


def main() -> None:
    merge_agency()
    merge_routes()
    merge_trips()
    merge_stops()
    merge_stop_times()
    merge_calendar()
    write_feed_info()
    cleanup_empty_optional_files()
    write_gtfs_zip()
    print("Combined GTFS updated successfully")


if __name__ == "__main__":
    main()
