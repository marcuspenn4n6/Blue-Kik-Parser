import sqlite3
import argparse
import os
import imghdr
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Parse command-line arguments
parser = argparse.ArgumentParser(description='Extract and save image BLOBs from an SQLite database.')
parser.add_argument('database', type=str, help='Path to the SQLite database file')
args = parser.parse_args()

# Generate output directory based on database name
output_dir = f"{os.path.splitext(os.path.basename(args.database))[0]}_images"
os.makedirs(output_dir, exist_ok=True)

# Connect to the SQLite database
conn = sqlite3.connect(args.database)
cursor = conn.cursor()

# Execute the SQL query to fetch images
cursor.execute("SELECT image_id, image_bytes FROM AccountSwitcherImgBackupTable")

# Process images in batches to optimize memory usage
batch_size = 100
while True:
    rows = cursor.fetchmany(batch_size)
    if not rows:
        break

    for image_id, image_bytes in rows:
        if image_bytes:  # Ensure the image is not empty or None
            try:
                # Determine file extension
                file_extension = imghdr.what(None, h=image_bytes) or "jpg"
                filename = os.path.join(output_dir, f"{image_id}.{file_extension}")
                
                # Write image data to file
                with open(filename, "wb") as file:
                    file.write(image_bytes)
                logging.info(f"Saved: {filename}")
            except Exception as e:
                logging.error(f"Error saving image {image_id}: {e}")
        else:
            logging.warning(f"Skipping {image_id}: No image data")

# Close the cursor and connection
cursor.close()
conn.close()

logging.info("Image extraction completed.")