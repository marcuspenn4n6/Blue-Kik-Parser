import sqlite3
import pandas as pd
import logging
import os
import argparse

# Configure logging
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s")

# Parse command-line arguments
parser = argparse.ArgumentParser(description="Generate an HTML report from a Kik backup database.")
parser.add_argument("db_path", help="Path to the SQLite database file")
parser.add_argument("image_folder", help="Path to the folder containing images")
args = parser.parse_args()

# Database and Output Paths
DB_PATH = args.db_path
IMAGE_FOLDER = args.image_folder
OUTPUT_HTML = os.path.join(os.path.dirname(DB_PATH), os.path.splitext(os.path.basename(DB_PATH))[0] + "_Report.html")

# SQL Queries
# SQL Queries
QUERIES = {
    "Private Messages": """
        SELECT m._id, m.bin_id, DATETIME(m.timestamp / 1000, 'unixepoch') AS timestamp, 
               CASE WHEN m.was_me = 1 THEN 'account owner' ELSE m.partner_jid END AS sender, 
               COALESCE(m.body, '[No Text]') AS body, 
               m.content_id, COALESCE(ai.image_id, kc.content_string) AS image_id
        FROM messagesTable m
        LEFT JOIN AccountSwitcherImgBackupTable ai ON m.content_id = ai.image_id
        LEFT JOIN KIKContentTable kc ON m.content_id = kc.content_id  -- Corrected join
        WHERE m.bin_id LIKE '%@talk.kik.com';
    """,

    "Group Messages": """
        SELECT m._id, m.bin_id, m.timestamp, 
               CASE WHEN m.was_me = 1 THEN 'account owner' ELSE m.partner_jid END AS sender, 
               COALESCE(m.body, '[No Text]') AS body, 
               m.content_id, ai.image_id
        FROM messagesTable m
        LEFT JOIN AccountSwitcherImgBackupTable ai ON m.content_id = ai.image_id
        WHERE m.bin_id LIKE '%@groups.kik.com';
    """,

    "Images": """
        SELECT CASE WHEN m.was_me = 1 THEN 'account owner' ELSE m.partner_jid END AS sender, 
               m.content_id, ai.image_id
        FROM messagesTable m
        LEFT JOIN AccountSwitcherImgBackupTable ai ON m.content_id = ai.image_id;
    """
}


def fetch_data_from_db(db_path, queries):
    """Fetches data from the SQLite database and returns a dictionary of DataFrames."""
    try:
        conn = sqlite3.connect(db_path)
        data_frames = {key: pd.read_sql_query(query, conn) for key, query in queries.items()}
        conn.close()
        logging.info("Successfully fetched data from database.")
        return data_frames
    except Exception as e:
        logging.error(f"Error fetching data from database: {e}")
        raise RuntimeError("Critical database error. Execution halted.")


def generate_html(data_frames, output_file, image_folder):
    """Generates an HTML report with tab navigation and saves it to a file."""
    try:
        html_content = """
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>Kik Backup Report</title>
            <style>
                body { font-family: Arial, sans-serif; margin: 20px; }
                .tab { display: none; }
                .tab.active { display: block; }
                .tabs { display: flex; cursor: pointer; }
                .tab-button { padding: 10px; border: 1px solid #ddd; background: #f0f0f0; margin-right: 5px; }
                table { width: 100%; border-collapse: collapse; margin-bottom: 20px; }
                th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }
                th { background-color: #f4f4f4; }
                img { max-width: 150px; height: auto; display: block; }
            </style>
            <script>
                function showTab(tabId) {
                    document.querySelectorAll('.tab').forEach(tab => tab.classList.remove('active'));
                    document.getElementById(tabId).classList.add('active');
                }
            </script>
        </head>
        <body>
            <h1>Kik Backup Report</h1>
            <div class="tabs">
                <div class="tab-button" onclick="showTab('private_messages')">Private Messages</div>
                <div class="tab-button" onclick="showTab('group_messages')">Group Messages</div>
                <div class="tab-button" onclick="showTab('images')">Images</div>
            </div>
        """

        for section, df in data_frames.items():
            section_id = section.lower().replace(" ", "_")
            html_content += f"<div id='{section_id}' class='tab'><h2>{section}</h2><table><tr>"
            html_content += "".join(f"<th>{col}</th>" for col in df.columns)
            html_content += "</tr>"

            for _, row in df.iterrows():
                html_content += "<tr>"
                for col in df.columns:
                    if col == "image_id" and pd.notna(row[col]):
                        image_extensions = ['jpg', 'jpeg', 'png', 'gif']
                        image_path = None
                        for ext in image_extensions:
                            potential_path = os.path.join(image_folder, f"{row[col]}.{ext}")
                            if os.path.exists(potential_path):
                                image_path = potential_path
                                break
                        if not image_path:
                            image_path = 'Image Not Found'
                            logging.warning(f"Missing image: {row[col]}")
                        else:
                            logging.info(f"Image found: {image_path}")
                        html_content += f"<td><img src='{image_path}' alt='Image'></td>"
                html_content += "</tr>"
            html_content += "</table></div>"

        html_content += "<script>showTab('private_messages');</script></body></html>"

        with open(output_file, "w", encoding="utf-8") as f:
            f.write(html_content)

        logging.info(f"HTML report successfully generated: {output_file}")
    except Exception as e:
        logging.error(f"Error generating HTML report: {e}")
        raise

if __name__ == "__main__":
    logging.info("Starting script execution...")
    data_frames = fetch_data_from_db(DB_PATH, QUERIES)
    if data_frames:
        generate_html(data_frames, OUTPUT_HTML, IMAGE_FOLDER)
    else:
        logging.warning("No data extracted from the database.")
    logging.info("Script execution finished.")
