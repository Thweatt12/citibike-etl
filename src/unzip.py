import os
import glob
import zipfile
DATA_DIR = r"C:\Users\brand\Downloads\citibike"
def main():
    if not os.path.isdir(DATA_DIR):
        raise FileNotFoundError(f"Folder not found: {DATA_DIR}")

    zips = glob.glob(os.path.join(DATA_DIR, "*.zip"))
    if not zips:
        print("No .zip files found. Nothing to do.")
        return

    print(f"Found {len(zips)} zip(s). Extracting...")
    for z in zips:
        print(f" - {os.path.basename(z)}")
        with zipfile.ZipFile(z, "r") as ref:
            ref.extractall(DATA_DIR)

    csvs = glob.glob(os.path.join(DATA_DIR, "**", "*.csv"), recursive=True)
    print(f"Done. Found {len(csvs)} CSV(s). Example:")
    for p in csvs[:5]:
        print("  ", p)

if __name__ == "__main__":
    main()