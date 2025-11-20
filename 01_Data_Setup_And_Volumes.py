# Databricks notebook source
# MAGIC %sql
# MAGIC USE CATALOG workspace;
# MAGIC
# MAGIC -- 1. Create a Schema specifically for file ingestion (distinct from 'bronze' tables)
# MAGIC CREATE SCHEMA IF NOT EXISTS landing;
# MAGIC
# MAGIC -- 2. Create a Volume named 'inbox'
# MAGIC CREATE VOLUME IF NOT EXISTS landing.inbox;

# COMMAND ----------

import multiprocessing
import os 

VOLUME_ROOT = "/Volumes/workspace/landing/inbox"
TARGET_FOLDER_NAME = "source_v1" 
GITHUB_ZIP_URL = "https://github.com/Yogita-Ganage/netflix-imdb-movies-analytics/releases/download/v1/raw_files.zip"

FINAL_DESTINATION = f"{VOLUME_ROOT}/{TARGET_FOLDER_NAME}"
ZIP_PATH = f"{VOLUME_ROOT}/download.zip"

# MAX_THREADS based on available CPU cores
MAX_THREADS = min(32, max(4, multiprocessing.cpu_count()))

def extract_file_worker(zip_path, file_info, target_root):
    """
    Extracts a single file from the zip archive to the target directory.
    """
    try:
        target_path = os.path.join(target_root, file_info.filename)
        os.makedirs(os.path.dirname(target_path), exist_ok=True)
        with zipfile.ZipFile(zip_path, 'r') as zf:
            with zf.open(file_info) as source, open(target_path, "wb") as target:
                shutil.copyfileobj(source, target)
        return True
    except Exception as e:
        print(f"Error extracting {file_info.filename}: {e}")
        return False

def bootstrap_threaded():
    if os.path.exists(f"{FINAL_DESTINATION}/netflix/titles.csv"):
        print(f"Valid data exists at {FINAL_DESTINATION}. Skipping.")
        print_paths()
        return

    print(f"Initializing Parallel Ingestion (Batch Size: {MAX_THREADS})...")
    
    if os.path.exists(FINAL_DESTINATION):
        shutil.rmtree(FINAL_DESTINATION)

    try:
        print("   1. Downloading stream to Volume...")
        urllib.request.urlretrieve(GITHUB_ZIP_URL, ZIP_PATH)
        
        print("   2. Analyzing Zip structure...")
        with zipfile.ZipFile(ZIP_PATH, 'r') as zip_ref:
            all_files = zip_ref.infolist()
            files_to_extract = [f for f in all_files if not f.is_dir()]
            temp_extract_path = f"{VOLUME_ROOT}/_temp_parallel"
            
        print(f"   3. Extracting {len(files_to_extract)} files using {MAX_THREADS} threads...")
        with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
            futures = [
                executor.submit(extract_file_worker, ZIP_PATH, f, temp_extract_path) 
                for f in files_to_extract
            ]
            for future in futures:
                future.result()

        print("   4. Finalizing structure...")
        items = os.listdir(temp_extract_path)
        if not items:
            raise Exception("Extraction failed.")
        extracted_folder = items[0]
        shutil.move(f"{temp_extract_path}/{extracted_folder}", FINAL_DESTINATION)
        shutil.rmtree(temp_extract_path)

        print("Success.")
        print_paths()

    except Exception as e:
        print(f"Error: {e}")
        if os.path.exists(FINAL_DESTINATION):
            shutil.rmtree(FINAL_DESTINATION)
        raise e
    finally:
        if os.path.exists(ZIP_PATH):
            os.remove(ZIP_PATH)

def print_paths():
    print(f"{FINAL_DESTINATION}/netflix")
    print(f"{FINAL_DESTINATION}/imdb")

bootstrap_threaded()
