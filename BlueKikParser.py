import sqlite3
import pandas as pd
import argparse
import logging
from datetime import datetime, timezone
import os

# Configure logging with debug level
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Function to convert UNIX milliseconds timestamp to human-readable format
def convert_to_human_readable(unix_timestamp_ms):
    try:
        unix_timestamp = unix_timestamp_ms / 1000.0
        return datetime.fromtimestamp(unix_timestamp, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
    except (OSError, ValueError) as e:
        logging.error(f"Error converting timestamp {unix_timestamp_ms}: {e}")
        return None

# Parse command-line arguments
parser = argparse.ArgumentParser(description='Extract and export messages from an SQLite database.')
parser.add_argument('database', type=str, help='Path to the SQLite database file')
args = parser.parse_args()

# Generate output filename based on database argument
output_file = f"Blue Kik Parsed - {os.path.basename(args.database)}.xlsx"

# Connect to the SQLite database
logging.debug(f"Connecting to database: {args.database}")
conn = sqlite3.connect(args.database)
cursor = conn.cursor()

# Function to check if a column exists in a table
def column_exists(table, column):
    cursor.execute(f"PRAGMA table_info({table})")
    columns = [row[1] for row in cursor.fetchall()]
    logging.debug(f"Columns in {table}: {columns}")
    return column in columns

# Ensure partner_jid exists in messagesTable
partner_jid_column = "partner_jid" if column_exists("messagesTable", "partner_jid") else None
if not partner_jid_column:
    logging.error("Required column 'partner_jid' not found in messagesTable. Exiting.")
    exit(1)

queries = {
    "Private Messages": f"""
        SELECT CASE WHEN was_me = 1 THEN 'account owner' ELSE {partner_jid_column} END AS sender, 
               body, timestamp, was_me
        FROM messagesTable
        WHERE {partner_jid_column} NOT IN (SELECT group_id FROM memberTable)
    """,
    "Group Messages": f"""
        SELECT memberTable.group_id, 
               CASE WHEN messagesTable.was_me = 1 THEN 'account owner' ELSE messagesTable.{partner_jid_column} END AS sender, 
               COALESCE(messagesTable.body, '[No Text]') AS body, messagesTable.timestamp, messagesTable.was_me
        FROM messagesTable
        JOIN memberTable ON messagesTable.bin_id = memberTable.group_id
        WHERE memberTable.group_id IS NOT NULL
    """,
    "Images & Content": f"""
        SELECT CASE WHEN messagesTable.was_me = 1 THEN 'account owner' ELSE messagesTable.{partner_jid_column} END AS sender, 
               messagesTable.content_id, KIKContentTable.content_name, KIKContentURITable.content_uri, 
               AccountSwitcherImgBackupTable.image_id, messagesTable.was_me, 
               KIKContentRetainCountTable.retain_count
        FROM messagesTable
        LEFT JOIN KIKContentTable ON messagesTable.content_id = KIKContentTable.content_id
        LEFT JOIN KIKContentURITable ON messagesTable.content_id = KIKContentURITable.content_id
        LEFT JOIN AccountSwitcherImgBackupTable ON KIKContentTable.content_string = AccountSwitcherImgBackupTable.image_id
        LEFT JOIN KIKContentRetainCountTable ON messagesTable.content_id = KIKContentRetainCountTable.content_id
        WHERE AccountSwitcherImgBackupTable.image_id IS NOT NULL AND KIKContentURITable.content_uri IS NULL
    """
}

# Process and export queries in chunks
with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
    for sheet_name, query in queries.items():
        logging.info(f"Processing {sheet_name}...")
        logging.debug(f"Executing query for {sheet_name}: {query}")
        try:
            chunk_iter = pd.read_sql_query(query, conn, chunksize=1000)
            df_list = []
            for chunk in chunk_iter:
                logging.debug(f"Fetched {len(chunk)} rows for {sheet_name}")
                if 'timestamp' in chunk.columns:
                    chunk['timestamp'] = chunk['timestamp'].apply(convert_to_human_readable)
                df_list.append(chunk)
            full_df = pd.concat(df_list, ignore_index=True) if df_list else pd.DataFrame()
            full_df.to_excel(writer, sheet_name=sheet_name, index=False)
        except Exception as e:
            logging.error(f"Error processing {sheet_name}: {e}")

# Close the cursor and connection
logging.debug("Closing database connection.")
cursor.close()
conn.close()
logging.info(f"Enhanced data export completed: {output_file}")
