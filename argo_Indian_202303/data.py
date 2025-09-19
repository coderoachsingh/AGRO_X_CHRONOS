import os
import pandas as pd
import xarray as xr
import numpy as np
from urllib.parse import urljoin, urlparse
from io import BytesIO
import mysql.connector
import gzip
from ftplib import FTP

# --------------------
# PARAMETERS
# --------------------
lat_min, lat_max = -5, 5
lon_min, lon_max = 60, 80
start_date = "2023-03-01"
end_date = "2023-03-31"

FTP_BASE_URL = "ftp://ftp.ifremer.fr/ifremer/argo/"
INDEX_FILE_URL = urljoin(FTP_BASE_URL, "ar_index_global_prof.txt.gz")

download_folder = "argo_data_downloads"
os.makedirs(download_folder, exist_ok=True)

# --------------------
# HELPER FUNCTION FOR FTP DOWNLOADS
# --------------------
def download_file_ftp(file_url, dest_path):
    """Downloads a single file from an FTP URL."""
    try:
        url_parts = urlparse(file_url)
        ftp_server = url_parts.netloc
        # FTP path needs to start from the root, so we remove the leading '/'
        ftp_path = os.path.dirname(url_parts.path).lstrip('/')
        file_name = os.path.basename(url_parts.path)

        with FTP(ftp_server) as ftp:
            ftp.login() # Anonymous login
            ftp.cwd(ftp_path) # Change to the correct directory
            with open(dest_path, "wb") as f:
                ftp.retrbinary(f"RETR {file_name}", f.write)
        return True
    except Exception as e:
        print(f"  -> FTP Download Failed for {file_url}: {e}")
        return False

# --------------------
# STEP D: FETCH AND PARSE THE GLOBAL INDEX
# --------------------
print("Fetching global ARGO index via FTP:", INDEX_FILE_URL)
url_parts = urlparse(INDEX_FILE_URL)
ftp_server = url_parts.netloc
ftp_path = os.path.dirname(url_parts.path).lstrip('/')
file_name = os.path.basename(url_parts.path)
ftp_file_buffer = BytesIO()

try:
    with FTP(ftp_server) as ftp:
        ftp.login()
        ftp.cwd(ftp_path)
        ftp.retrbinary(f"RETR {file_name}", ftp_file_buffer.write)
    ftp_file_buffer.seek(0)
    with gzip.open(ftp_file_buffer, 'rt') as f:
        df_index = pd.read_csv(f, comment='#', header=0)
except Exception as e:
    print(f"An error occurred during FTP index download: {e}")
    exit()

df_index['date'] = pd.to_datetime(df_index['date'], format='%Y%m%d%H%M%S')
print(f"Filtering {len(df_index)} total profiles...")
filtered = df_index[
    (df_index['latitude'] >= lat_min) & (df_index['latitude'] <= lat_max) &
    (df_index['longitude'] >= lon_min) & (df_index['longitude'] <= lon_max) &
    (df_index['date'] >= start_date) & (df_index['date'] <= end_date)
].copy()
print(f"Found {len(filtered)} profiles matching your criteria.")

# --------------------
# STEP E: DOWNLOAD DATA FILES
# --------------------
downloaded_files = []
for idx, row in filtered.iterrows():
    file_path = row['file']
    fname = os.path.basename(file_path)
    # Correctly join the base FTP URL with the relative file path from the index
    file_url = urljoin(FTP_BASE_URL, file_path)
    dest = os.path.join(download_folder, fname)

    if not os.path.exists(dest):
        print(f"Downloading: {fname}")
        # Use our dedicated FTP download function
        if download_file_ftp(file_url, dest):
            downloaded_files.append(dest)
    else:
        print(f"Already exists: {fname}")
        downloaded_files.append(dest)

# --------------------
# STEP F: PROCESS DOWNLOADED FILES
# --------------------
def process_nc_file(nc_path, vars_to_keep=["TEMP", "PSAL"]):
    try:
        ds = xr.open_dataset(nc_path)
    except Exception as e:
        print(f"Error opening {nc_path}: {e}")
        return pd.DataFrame()

    float_id = str(ds["PLATFORM_NUMBER"].values.item())
    times = pd.to_datetime(ds["JULD"].values, origin="1950-01-01", unit="D")
    lat = ds["LATITUDE"].values
    lon = ds["LONGITUDE"].values
    depth = ds["PRES"].values

    rows = []
    for i in range(ds.dims['N_PROF']):
        for j in range(ds.dims['N_LEVELS']):
            record = {
                "float_id": float_id, "date": times[i], "latitude": float(lat[i]),
                "longitude": float(lon[i]), "depth": float(depth[i, j]),
            }
            for var in vars_to_keep:
                qc_var = var + "_QC"
                if qc_var in ds and ds[qc_var].values[i, j] in [b'1', b'2']:
                    record[var.lower()] = float(ds[var].values[i, j])

            if len(record) > 5:
                 rows.append(record)

    return pd.DataFrame(rows)

all_rows = []
for fpath in downloaded_files:
    print("Processing:", fpath)
    df = process_nc_file(fpath)
    if not df.empty:
        all_rows.append(df)

if not all_rows:
    print("No data was successfully processed. Exiting.")
    exit()

combined = pd.concat(all_rows, ignore_index=True)
combined.rename(columns={"temp": "temperature", "psal": "salinity"}, inplace=True)
print("Final dataset shape:", combined.shape)
csv_path = "argo_processed.csv"
combined.to_csv(csv_path, index=False)
print(f"Processed data saved to {csv_path}")

# --------------------
# STEP G: LOAD INTO MYSQL
# --------------------
try:
    conn = mysql.connector.connect(
        host="localhost", user="root", password="Arman123?",
        database="argo_data", allow_local_infile=True
    )
    cursor = conn.cursor()
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS argo_profiles (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        float_id VARCHAR(50), date DATETIME, latitude DOUBLE, longitude DOUBLE,
        depth DOUBLE, temperature DOUBLE, salinity DOUBLE,
        INDEX idx_date (date), INDEX idx_latlon (latitude, longitude), INDEX idx_float (float_id)
    );
    """)
    cursor.execute("TRUNCATE TABLE argo_profiles;")
    print("Cleared existing data from argo_profiles table.")
    csv_full_path = os.path.abspath(csv_path).replace("\\", "/")
    load_sql = f"LOAD DATA LOCAL INFILE '{csv_full_path}' INTO TABLE argo_profiles FIELDS TERMINATED BY ',' ENCLOSED BY '\"' LINES TERMINATED BY '\\n' IGNORE 1 ROWS (float_id, date, latitude, longitude, depth, temperature, salinity);"
    cursor.execute(load_sql)
    conn.commit()
    print(f"Successfully loaded {cursor.rowcount} rows into MySQL.")
except mysql.connector.Error as err:
    print(f"Error connecting to or loading data into MySQL: {err}")
finally:
    if 'conn' in locals() and conn.is_connected():
        cursor.close()
        conn.close()
        print("MySQL connection closed.")