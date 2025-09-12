import sys, psycopg2

# adjust for container use: host="postgres" if running inside Docker
PG = dict(host="postgres", port=5432, dbname="Ctit_Data", user="airflow", password="airflow")

def main():
    if len(sys.argv) < 2:
        raise SystemExit("Usage: python run_sql.py /path/to/file.sql")
    sql_file = sys.argv[1]
    with open(sql_file, "r") as f:
        sql = f.read()
    conn = psycopg2.connect(**PG)
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()
        print(f"Executed SQL from {sql_file}")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
