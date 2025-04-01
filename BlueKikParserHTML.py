import os
import sys
import json
import logging
import argparse
import sqlite3
from typing import Tuple, List, Dict, Optional

import pandas as pd
from jinja2 import Environment

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def scan_folder(scan_path: str) -> Tuple[List[Tuple[str, str, str]], Optional[str]]:
    backups = []
    json_file = None
    for file in os.listdir(scan_path):
        full_path = os.path.join(scan_path, file)
        if file.endswith(".backup"):
            base = file
            image_csv = os.path.join(scan_path, f"{base}_image_index.csv")
            image_dir = os.path.join(scan_path, f"{base}_images")
            backups.append((full_path, image_csv, image_dir))
        elif file.endswith(".json") and json_file is None:
            json_file = full_path
    return backups, json_file


def load_category_map(json_path: str) -> Dict[str, str]:
    if not json_path or not os.path.exists(json_path):
        logging.warning("No category JSON provided or file not found.")
        return {}
    try:
        with open(json_path, "r", encoding="utf-8-sig") as f:
            data = json.load(f)
        category_map = {}
        # Iterate over each case then each media entry in the "Media" list
        for case in data.get("value", []):
            for media in case.get("Media", []):
                category = str(media.get("Category", "")).strip()
                for file_entry in media.get("MediaFiles", []):
                    fname = file_entry.get("FileName", "").strip().lower()
                    if fname.endswith((".jpg", ".jpeg", ".png", ".gif")):
                        fname = os.path.splitext(fname)[0]
                    if fname:
                        category_map[fname] = category
        return category_map
    except Exception as e:
        logging.error(f"Failed to load category JSON: {e}")
        return {}


def fetch_data_from_db(db_path: str, queries: Dict[str, str]) -> Dict[str, pd.DataFrame]:
    try:
        with sqlite3.connect(db_path) as conn:
            frames = {name: pd.read_sql_query(query, conn) for name, query in queries.items()}
        return frames
    except Exception as e:
        logging.error(f"Failed to query {db_path}: {e}")
        return {}


def process_backup(db_path: str, image_csv: str, image_dir: str, category_map: Dict[str, str]) -> Optional[Dict[str, pd.DataFrame]]:
    if not os.path.exists(image_csv):
        logging.warning(f"Missing image index CSV: {image_csv}")
        return None

    try:
        image_index = pd.read_csv(image_csv)
        if not {"image_id", "filename"}.issubset(image_index.columns):
            logging.error(f"Image index CSV {image_csv} missing required columns.")
            return None
        # Create lookup dictionary using vectorized method
        image_lookup = image_index.set_index("image_id")["filename"].to_dict()
    except Exception as e:
        logging.error(f"Error reading image index {image_csv}: {e}")
        return None

    queries = {
        "Private Messages": """
            SELECT m._id, m.bin_id, DATETIME(m.timestamp / 1000, 'unixepoch') AS timestamp,
                   CASE WHEN m.was_me = 1 THEN 'account owner' ELSE m.partner_jid END AS sender,
                   COALESCE(m.body, '[No Text]') AS body,
                   m.content_id, kc.content_name, ku.content_uri, ai.image_id
            FROM messagesTable m
            LEFT JOIN KIKContentTable kc ON m.content_id = kc.content_id
            LEFT JOIN KIKContentURITable ku ON m.content_id = ku.content_id
            LEFT JOIN AccountSwitcherImgBackupTable ai ON kc.content_string = ai.image_id
            WHERE m.bin_id LIKE '%@talk.kik.com'
            AND (kc.content_name = 'preview' OR kc.content_name IS NULL);
        """,
        "Group Messages": """
            SELECT m._id, m.bin_id, DATETIME(m.timestamp / 1000, 'unixepoch') AS timestamp,
                   CASE WHEN m.was_me = 1 THEN 'account owner' ELSE m.partner_jid END AS sender,
                   COALESCE(m.body, '[No Text]') AS body,
                   m.content_id, kc.content_name, ku.content_uri, ai.image_id
            FROM messagesTable m
            LEFT JOIN KIKContentTable kc ON m.content_id = kc.content_id
            LEFT JOIN KIKContentURITable ku ON m.content_id = ku.content_id
            LEFT JOIN AccountSwitcherImgBackupTable ai ON kc.content_string = ai.image_id
            WHERE m.bin_id LIKE '%@groups.kik.com'
            AND (kc.content_name = 'preview' OR kc.content_name IS NULL);
        """,
        "Images": """
            SELECT CASE WHEN m.was_me = 1 THEN 'account owner' ELSE m.partner_jid END AS sender,
                   m.content_id, kc.content_name, ku.content_uri, ai.image_id
            FROM messagesTable m
            LEFT JOIN KIKContentTable kc ON m.content_id = kc.content_id
            LEFT JOIN KIKContentURITable ku ON m.content_id = ku.content_id
            LEFT JOIN AccountSwitcherImgBackupTable ai ON kc.content_string = ai.image_id
            LEFT JOIN KIKContentRetainCountTable kr ON m.content_id = kr.content_id
            WHERE (m.bin_id LIKE '%@talk.kik.com' OR m.bin_id LIKE '%@groups.kik.com')
            AND kc.content_name = 'preview';
        """
    }

    data_frames = fetch_data_from_db(db_path, queries)
    if not data_frames:
        return None

    for name, df in data_frames.items():
        if "image_id" in df.columns:
            # Vectorized creation of image_filename, image_path and category
            df["image_filename"] = df["image_id"].astype(str).map(image_lookup)
            df["image_path"] = df["image_filename"].apply(
                lambda x: os.path.join(os.path.basename(image_dir), x) if pd.notna(x) else None
            )
            df["category"] = (
                df["image_filename"]
                .str.lower()
                .str.replace(r'\.(jpg|jpeg|png|gif)$', '', regex=True)
                .map(category_map)
                .fillna("")
            )
            df.drop(columns=["image_filename"], inplace=True)
    return data_frames


def fetch_all_data(backups: List[Tuple[str, str, str]], category_map: Dict[str, str]) -> Dict[str, pd.DataFrame]:
    accumulator: Dict[str, List[pd.DataFrame]] = {"Private Messages": [], "Group Messages": [], "Images": []}
    for db_path, image_csv, image_dir in backups:
        result = process_backup(db_path, image_csv, image_dir, category_map)
        if not result:
            logging.error(f"Failed to process {db_path}")
            continue
        for section in accumulator:
            if section in result and not result[section].empty:
                accumulator[section].append(result[section])
    combined: Dict[str, pd.DataFrame] = {}
    for section, df_list in accumulator.items():
        if df_list:
            combined_df = pd.concat(df_list, ignore_index=True)
            # Rename _id to id so that it maps correctly in the template
            if "_id" in combined_df.columns:
                combined_df.rename(columns={"_id": "id"}, inplace=True)
            combined[section] = combined_df
        else:
            combined[section] = pd.DataFrame()
    return combined


def render_html(sections: Dict[str, pd.DataFrame], output_path: str) -> None:
    env = Environment()
    template_str = '''
    <!DOCTYPE html>
    <html>
    <head>
      <meta charset="UTF-8">
      <title>Kik Backup Report</title>
      <style>
        body { font-family: Arial; margin: 20px; }
        .tabs { display: flex; margin-bottom: 10px; }
        .tab-button { margin-right: 10px; padding: 8px; background: #eee; border: 1px solid #ccc; cursor: pointer; }
        .tab { display: none; }
        .tab.active { display: block; }
        table { width: 100%; border-collapse: collapse; margin-bottom: 20px; }
        th, td { padding: 6px; border: 1px solid #ccc; }
        img { max-width: 150px; }
      </style>
      <script>
        function showTab(tabId) {
          document.querySelectorAll('.tab').forEach(el => el.classList.remove('active'));
          document.getElementById(tabId).classList.add('active');
        }
      </script>
    </head>
    <body>
    <h1>Kik Backup Report</h1>
    <div class="tabs">
    {% for name in sections %}
      <div class="tab-button" onclick="showTab('{{ name | replace(' ', '_') }}')">{{ name }}</div>
    {% endfor %}
    </div>
    {% for name, df in sections.items() %}
    <div id="{{ name | replace(' ', '_') }}" class="tab">
      <h2>{{ name }}</h2>
      <table>
        <tr>{% for col in df.columns %}<th>{{ col }}</th>{% endfor %}</tr>
        {% for row in df.itertuples(index=False) %}
        {% set row_dict = row._asdict() %}
        <tr>
          {% for col in df.columns %}
            {% if col == "image_path" and row_dict[col] %}
              <td><img src="{{ row_dict[col] }}" alt="image"></td>
            {% else %}
              <td>{{ row_dict[col] }}</td>
            {% endif %}
          {% endfor %}
        </tr>
        {% endfor %}
      </table>
    </div>
    {% endfor %}
    <script>showTab('{{ sections.keys()|list|first | replace(' ', '_') }}')</script>
    </body>
    </html>
    '''
    template = env.from_string(template_str)
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(template.render(sections=sections))
    logging.info(f"HTML written to: {output_path}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("folder", help="Folder containing .backup, .csv, .json and image folders")
    parser.add_argument("--categories", help="Comma-separated list of category values to include", default=None)
    args = parser.parse_args()

    if args.categories:
        category_filter = [c.strip() for c in args.categories.split(",")]
        logging.info(f"Filtering by categories: {category_filter}")
    else:
        category_filter = None

    backups, json_path = scan_folder(args.folder)
    if not backups:
        logging.error("No valid database/image index pairs found.")
        sys.exit(1)

    category_map = load_category_map(json_path)
    combined = fetch_all_data(backups, category_map)

    if category_filter:
        for section in combined:
            if not combined[section].empty and "category" in combined[section].columns:
                before = len(combined[section])
                combined[section] = combined[section][combined[section]["category"].isin(category_filter)]
                after = len(combined[section])
                logging.info(f"Filtered '{section}': {before} → {after} rows matching category filter")
        if all(df.empty for df in combined.values()):
            logging.warning("No rows matched the specified category filter.")

    db_dir = os.path.dirname(backups[0][0])
    output_path = os.path.join(db_dir, "Combined_Kik_Report.html")
    render_html(combined, output_path)


if __name__ == "__main__":
    main()
