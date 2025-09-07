import os, glob, csv, io, psycopg2

DATA_DIR = r"C:\Users\brand\Downloads\citibike"
PG = dict(host="localhost", port=5432, dbname="Ctit_Data", user="airflow", password="airflow")

HEADERS = [
    "tripduration","starttime","stoptime",
    "start_station_id","start_station_name",
    "start_station_latitude","start_station_longitude",
    "end_station_id","end_station_name",
    "end_station_latitude","end_station_longitude",
    "bikeid","usertype","birth_year","gender"
]

HEADER_MAP = {
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
}

DDL = """
CREATE TABLE IF NOT EXISTS stg_citibike (
    tripduration BIGINT,
    starttime TIMESTAMP,
    stoptime TIMESTAMP,
    start_station_id INT,
    start_station_name TEXT,
    start_station_latitude DOUBLE PRECISION,
    start_station_longitude DOUBLE PRECISION,
    end_station_id INT,
    end_station_name TEXT,
    end_station_latitude DOUBLE PRECISION,
    end_station_longitude DOUBLE PRECISION,
    bikeid BIGINT,
    usertype TEXT,
    birth_year INT,
    gender INT
);
"""

# 👇 Add these BEFORE connect()
NULL_TOKENS = {"", "NULL", "null", "\\N", "N/A", "na"}

def normalize_station_id(val):
    if not val or val.strip() in NULL_TOKENS:
        return ""
    try:
        return str(int(float(val)))
    except ValueError:
        return ""

def normalize_value(v: str) -> str:
    if v is None:
        return ""
    s = v.strip()
    return "" if s in NULL_TOKENS else s

def normalize_row(row_dict):
    raw = { (k or "").strip().lower(): (v or "") for k,v in row_dict.items() }
    clean = { c: "" for c in HEADERS }
    for k, v in raw.items():
        mapped = HEADER_MAP.get(k)
        if mapped:
            clean[mapped] = normalize_value(v)
    if mapped in ("start_station_id", "end_station_id"):
        clean[mapped] = normalize_station_id(v)
    else: clean[mapped] = normalize_value(v)
    return clean

# 👇 Now your connect + rest
def connect():
    return psycopg2.connect(**PG)

def create_table(conn):
    with conn.cursor() as cur:
        cur.execute(DDL)
    conn.commit()

def copy_csvs(conn, csv_paths):
    for path in csv_paths:
        print(f"Loading {os.path.basename(path)} ...")
        with open(path, newline="", encoding="utf-8", errors="ignore") as src:
            reader = csv.DictReader(src)
            buf = io.StringIO()
            writer = csv.DictWriter(buf, fieldnames=HEADERS)
            writer.writeheader()
            rows = 0
            for row in reader:
                writer.writerow(normalize_row(row))   # 👈 cleaned
                rows += 1
            if rows == 0:
                print("  (empty) skipping")
                continue
            buf.seek(0)
            with conn.cursor() as cur:
                cur.copy_expert(
                    f"COPY stg_citibike ({','.join(HEADERS)}) "
                    "FROM STDIN WITH (FORMAT CSV, HEADER TRUE, NULL '')",   # 👈 important
                    buf
                )
            conn.commit()
            print(f"  {rows} rows loaded.")

def main():
    csvs = glob.glob(os.path.join(DATA_DIR, "**", "*.csv"), recursive=True)
    if not csvs:
        print("No CSVs found.")
        return
    conn = connect()
    try:
        create_table(conn)
        copy_csvs(conn, csvs)
    finally:
        conn.close()

if __name__ == "__main__":
    main()
