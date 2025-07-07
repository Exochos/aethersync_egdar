import os
import gzip
import shutil
import logging
import tempfile
import datetime
import requests
from pymongo import MongoClient
from logging.handlers import RotatingFileHandler

# === CONFIG ===
BASE_URL = "https://www.sec.gov/Archives/edgar/daily-index"
HEADERS = {"User-Agent": "AetherSyncBot/1.0 (mailto:jeremy@aethersync.io)"}
TARGET_FORMS = {"10-K", "10-Q", "13F", "13D", "4"}
MONGO_URI = "mongodb://mongodb:27017"  # or 'localhost' outside Docker
DB_NAME = "edgar"
COLLECTION_NAME = "filings"
LOG_FILE = "edgar_ingest.log"

# === LOGGING ===
logger = logging.getLogger("aethersync_edgar_ingest")
logger.setLevel(logging.INFO)
handler = RotatingFileHandler(LOG_FILE, maxBytes=5_000_000, backupCount=3)
formatter = logging.Formatter('%(asctime)s %(levelname)s: %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

# === MONGO SETUP ===
client = MongoClient(MONGO_URI)
collection = client[DB_NAME][COLLECTION_NAME]

# === HELPERS ===

def get_today_idx_url():
    today = datetime.date.today()
    year = today.year
    quarter = f"QTR{(today.month - 1) // 3 + 1}"
    filename = f"master.{today.strftime('%Y%m%d')}.idx.gz"
    url = f"{BASE_URL}/{year}/{quarter}/{filename}"
    return url, filename

def download_gz(url, dest_path):
    logger.info(f"Downloading {url}")
    r = requests.get(url, headers=HEADERS, stream=True)
    if r.status_code != 200:
        raise Exception(f"Download failed with status {r.status_code}")
    with open(dest_path, "wb") as f:
        for chunk in r.iter_content(1024):
            f.write(chunk)

def decompress_gz(src_path, dest_path):
    with gzip.open(src_path, 'rb') as f_in, open(dest_path, 'wb') as f_out:
        shutil.copyfileobj(f_in, f_out)

def parse_idx(path):
    filings = []
    with open(path, "r", errors="ignore") as f:
        for line in f:
            if "|" in line:  # valid line
                parts = [p.strip() for p in line.strip().split('|')]
                if len(parts) == 5:
                    cik, company, form, date_filed, filename = parts
                    if form in TARGET_FORMS:
                        accession = filename.split('/')[-1].replace('.txt', '')
                        filings.append({
                            "cik": cik,
                            "company": company,
                            "form_type": form,
                            "date_filed": date_filed,
                            "accession": accession,
                            "file_url": f"https://www.sec.gov/Archives/{filename}"
                        })
    return filings

def fetch_filing_text(file_url):
    try:
        r = requests.get(file_url, headers=HEADERS, timeout=15)
        if r.status_code == 200:
            return r.text
        else:
            logger.warning(f"Failed to fetch filing text: {file_url}")
            return None
    except Exception as e:
        logger.error(f"Exception fetching {file_url}: {e}")
        return None

def insert_unique(filing):
    if collection.find_one({"accession": filing["accession"]}):
        return False
    filing["ingested_at"] = datetime.datetime.utcnow()
    text = fetch_filing_text(filing["file_url"])
    if text:
        filing["raw_text"] = text
        collection.insert_one(filing)
        return True
    return False

# === MAIN ===

def main():
    try:
        url, filename = get_today_idx_url()
        with tempfile.TemporaryDirectory() as tmpdir:
            gz_path = os.path.join(tmpdir, filename)
            idx_path = gz_path.replace(".gz", "")

            download_gz(url, gz_path)
            decompress_gz(gz_path, idx_path)

            filings = parse_idx(idx_path)
            logger.info(f"Parsed {len(filings)} filings")

            new_count = 0
            for filing in filings:
                if insert_unique(filing):
                    new_count += 1

            logger.info(f"Inserted {new_count} new filings into MongoDB")

    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)

if __name__ == "__main__":
    main()
