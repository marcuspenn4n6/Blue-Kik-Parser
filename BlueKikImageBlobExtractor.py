import sqlite3
import argparse
import os
import imghdr
import logging
import csv
from typing import Optional

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def detect_image_extension(image_bytes: bytes) -> str:
    """Detect image file extension using imghdr, fallback to jpg."""
    return imghdr.what(None, h=image_bytes) or "jpg"

def save_image(output_dir: str, image_id: str, image_bytes: bytes) -> Optional[str]:
    """Save a single image to disk and return its filename."""
    file_extension = detect_image_extension(image_bytes)
    filename = f"{image_id}.{file_extension}"
    filepath = os.path.join(output_dir, filename)

    try:
        with open(filepath, "wb") as file:
            file.write(image_bytes)
        logging.info(f"Saved: {filename}")
        return filename
    except Exception as e:
        logging.error(f"Error saving image {image_id}: {e}")
        return None

def extract_images_from_db(db_path: str):
    """Extract all image blobs from a single .backup file."""
    db_dir = os.path.dirname(os.path.abspath(db_path))
    db_file = os.path.basename(db_path)

    output_dir = os.path.join(db_dir, f"{db_file}_images")
    csv_path = os.path.join(db_dir, f"{db_file}_image_index.csv")

    logging.info(f"Processing database: {db_path}")
    logging.info(f"Output directory: {output_dir}")
    logging.info(f"CSV index path: {csv_path}")

    os.makedirs(output_dir, exist_ok=True)

    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT image_id, image_bytes FROM AccountSwitcherImgBackupTable")
    except sqlite3.Error as e:
        logging.error(f"SQL error in {db_path}: {e}")
        return

    with open(csv_path, mode='w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['image_id', 'filename'])

        while True:
            rows = cursor.fetchmany(100)
            if not rows:
                break

            for image_id, image_bytes in rows:
                if image_bytes:
                    filename = save_image(output_dir, image_id, image_bytes)
                    if filename:
                        writer.writerow([image_id, filename])
                else:
                    logging.warning(f"Skipping {image_id}: No image data")

    cursor.close()
    conn.close()
    logging.info(f"Finished extracting from: {db_path}")

def scan_and_extract(folder_or_file: str):
    """Determine whether to extract from a single file or scan a folder."""
    if os.path.isfile(folder_or_file) and folder_or_file.endswith('.backup'):
        extract_images_from_db(folder_or_file)
    elif os.path.isdir(folder_or_file):
        for root, _, files in os.walk(folder_or_file):
            for file in files:
                if file.endswith('.backup'):
                    db_path = os.path.join(root, file)
                    extract_images_from_db(db_path)
    else:
        logging.error(f"Invalid path: {folder_or_file}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Extract image BLOBs from a .backup file or folder of backups.')
    parser.add_argument('path', type=str, help='Path to a .backup file or folder containing .backup files')
    args = parser.parse_args()

    scan_and_extract(args.path)
