import os, glob, csv, io, zipfile, psycopg2

# >>> Adjust these as needed
DATA_DIR = "/opt/airflow/data/citibike"

PG = dict(host="postgres", port=5432, dbname="Ctit_Data", user="airflow", password="airflow")  # keep your DB name

DB_COLUMNS = [
    "tripduration","starttime","stoptime",
    "start_station_id","start_station_name",
    "start_station_latitude","start_station_longitude",
    "end_station_id","end_station_name",
    "end_station_latitude","end_station_longitude",
    "bikeid","usertype","birth_year","gender"
]

# Maps various header spellings to our DB column names; use None to ignore extra columns
HEADER_MAP = {
    # legacy schema
    "tripduration": "tripduration",
    "starttime": "starttime", "start time": "starttime",
    "stoptime": "stoptime", "stop time": "stoptime",
    "start station id": "start_station_id",
    "start station name": "start_station_name",
    "start station latitude": "start_station_latitude",
    "start station longitude": "start_station_longitude",
    "end station id": "end_station_id",
    "end station name": "end_station_name",
    "end station latitude": "end_station_latitude",
    "end station longitude": "end_station_longitude",
    "bikeid": "bikeid",
    "usertype": "usertype",
    "birth year": "birth_year",
    "gender": "gender",

    # newer schema
    "started_at": "starttime",
    "ended_at": "stoptime",
    "start_lat": "start_station_latitude",
    "start_lng": "start_station_longitude",
    "end_lat": "end_station_latitude",
    "end_lng": "end_station_longitude",
    "member_casual": "usertype",
    "rideable_type": None,   # ignore
}

NULL_TOKENS = {"", "NULL", "null", "\\N", "N/A", "na"}

def normalize_value(raw_value: str) -> str:
    if raw_value is None:
        return ""
    s = raw_value.strip()
    return "" if s in NULL_TOKENS else s

def normalize_station_id(raw_value: str) -> str:
    s = normalize_value(raw_value)
    if s == "":
        return ""
    try:
        return str(int(float(s)))  # "72.0" -> "72"
    except ValueError:
        return ""

def normalize_usertype(raw_value: str) -> str:
    s = normalize_value(raw_value).lower()
    if s in ("member", "subscriber", "annual member"):
        return "Subscriber"
    if s in ("casual", "customer", "one day", "day pass"):
        return "Customer"
    return s.title() if s else ""

def normalize_row(raw_row_dict: dict) -> dict:
    lowered = {(k or "").strip().lower(): (v or "") for k, v in raw_row_dict.items()}

    clean = {col: "" for col in DB_COLUMNS}  # blanks -> NULL in COPY
    for raw_key, raw_val in lowered.items():
        mapped_column = HEADER_MAP.get(raw_key)
        if mapped_column is None:
            continue  # ignore extras
        if mapped_column in ("start_station_id", "end_station_id"):
            clean[mapped_column] = normalize_station_id(raw_val)
        elif mapped_column == "usertype":
            clean[mapped_column] = normalize_usertype(raw_val)
        else:
            clean[mapped_column] = normalize_value(raw_val)
    return clean

def connect():
    return psycopg2.connect(**PG)

def copy_buffer(conn, buf, rows, source_name):
    if rows == 0:
        print(f"  ({source_name}) empty — skipping")
        return
    buf.seek(0)
    with conn.cursor() as cur:
        cur.copy_expert(
            f"COPY public.stg_citibike ({','.join(DB_COLUMNS)}) "
            "FROM STDIN WITH (FORMAT CSV, HEADER TRUE, NULL '')",
            buf
        )
    conn.commit()
    print(f"  ({source_name}) {rows} rows loaded.")

def process_reader(conn, reader, source_name):
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=DB_COLUMNS)
    writer.writeheader()
    rows = 0
    for raw in reader:
        writer.writerow(normalize_row(raw))
        rows += 1
    copy_buffer(conn, buf, rows, source_name)

def copy_csvs(conn, paths):
    for path in paths:
        ext = os.path.splitext(path)[1].lower()
        if ext == ".csv":
            print(f"Loading file: {os.path.basename(path)} ...")
            try:
                with open(path, newline="", encoding="utf-8", errors="ignore") as f:
                    reader = csv.DictReader(f)
                    process_reader(conn, reader, os.path.basename(path))
            except Exception as e:
                conn.rollback()
                print(f"  ERROR (file {os.path.basename(path)}): {e}")
        elif ext == ".zip":
            print(f"Loading zip:  {os.path.basename(path)} ...")
            try:
                with zipfile.ZipFile(path) as z:
                    # iterate CSV entries only
                    members = [m for m in z.namelist() if m.lower().endswith(".csv")]
                    if not members:
                        print("  (zip contains no CSVs) skipping")
                        continue
                    for m in members:
                        try:
                            print(f"  -> member: {m}")
                            with z.open(m) as fbin:
                                # decode bytes to text; ignore bad chars
                                with io.TextIOWrapper(fbin, encoding="utf-8", errors="ignore", newline="") as ftxt:
                                    reader = csv.DictReader(ftxt)
                                    process_reader(conn, reader, f"{os.path.basename(path)}::{m}")
                        except Exception as e_mem:
                            conn.rollback()
                            print(f"    ERROR (member {m}): {e_mem}")
            except zipfile.BadZipFile as e:
                print(f"  ERROR (bad zip {os.path.basename(path)}): {e}")
            except Exception as e:
                print(f"  ERROR (zip {os.path.basename(path)}): {e}")
        else:
            # ignore other extensions
            continue

def main():
    # find both CSVs and ZIP archives
    csvs = glob.glob(os.path.join(DATA_DIR, "**", "*.csv"), recursive=True)
    zips = glob.glob(os.path.join(DATA_DIR, "**", "*.zip"), recursive=True)
    paths = sorted(csvs + zips)
    if not paths:
        print("No CSVs or ZIPs found.")
        return

    conn = connect()
    try:
        copy_csvs(conn, paths)
    finally:
        conn.close()

if __name__ == "__main__":
    main()
