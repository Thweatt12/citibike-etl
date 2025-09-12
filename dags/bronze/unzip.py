import os, glob, zipfile

DATA_DIR = "/opt/airflow/data/citibike" # CONTAINER path

def main():
    if not os.path.isdir(DATA_DIR):
        raise FileNotFoundError(f"Folder not found: {DATA_DIR}")

    zips = glob.glob(os.path.join(DATA_DIR, "*.zip"))
    if not zips:
        print(f"No .zip files found in {DATA_DIR}. Nothing to do.")
        return

    print(f"Found {len(zips)} zip(s). Extracting to {DATA_DIR} ...")
    for z in zips:
        print(f" - {os.path.basename(z)}")
        with zipfile.ZipFile(z, "r") as ref:
            ref.extractall(DATA_DIR)
    print("Done.")

if __name__ == "__main__":
    main()
