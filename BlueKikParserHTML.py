import sqlite3
import pandas as pd
import logging
import os
import sys
import argparse
import json
import csv
from jinja2 import Environment

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

QUERIES = {
    "Private Messages": """
        SELECT m._id, m.bin_id, DATETIME(m.timestamp / 1000, 'unixepoch') AS timestamp,
               CASE WHEN m.was_me = 1 THEN 'account owner' ELSE m.partner_jid END AS sender,
               COALESCE(m.body, '[No Text]') AS body,
               m.content_id, kc.content_name, ku.content_uri,
               kc.content_string AS image_id
        FROM messagesTable m
        LEFT JOIN KIKContentTable kc ON m.content_id = kc.content_id
        LEFT JOIN KIKContentURITable ku ON m.content_id = ku.content_id
        WHERE m.bin_id LIKE '%@talk.kik.com'
        AND (kc.content_name = 'preview' OR kc.content_name IS NULL);
    """,
    "Group Messages": """
        SELECT m._id, m.bin_id, DATETIME(m.timestamp / 1000, 'unixepoch') AS timestamp,
               CASE WHEN m.was_me = 1 THEN 'account owner' ELSE m.partner_jid END AS sender,
               COALESCE(m.body, '[No Text]') AS body,
               m.content_id, kc.content_name, ku.content_uri,
               kc.content_string AS image_id
        FROM messagesTable m
        LEFT JOIN KIKContentTable kc ON m.content_id = kc.content_id
        LEFT JOIN KIKContentURITable ku ON m.content_id = ku.content_id
        WHERE m.bin_id LIKE '%@groups.kik.com'
        AND (kc.content_name = 'preview' OR kc.content_name IS NULL);
    """,
    "Images": """
        SELECT CASE WHEN m.was_me = 1 THEN 'account owner' ELSE m.partner_jid END AS sender,
               m.content_id, kc.content_name, ku.content_uri,
               kc.content_string AS image_id
        FROM messagesTable m
        LEFT JOIN KIKContentTable kc ON m.content_id = kc.content_id
        LEFT JOIN KIKContentURITable ku ON m.content_id = ku.content_id
        WHERE m.bin_id LIKE '%@talk.kik.com'
        AND kc.content_name = 'preview';
    """
}

def scan_folder(scan_path):
    backups = []
    json_file = None
    for file in os.listdir(scan_path):
        full_path = os.path.join(scan_path, file)
        if file.endswith(".backup"):
            image_csv = os.path.join(scan_path, f"{file}_image_index.csv")
            image_dir = os.path.join(scan_path, f"{file}_images")
            if os.path.exists(image_csv) and os.path.exists(image_dir):
                backups.append((full_path, image_csv, image_dir))
            else:
                if not os.path.exists(image_csv):
                    logging.warning(f"Missing image index CSV: {image_csv}")
                if not os.path.exists(image_dir):
                    logging.warning(f"Missing image folder: {image_dir}")
        elif file.endswith(".json") and json_file is None:
            json_file = full_path
    return backups, json_file

def load_image_index(index_path):
    image_map = {}
    try:
        with open(index_path, newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                image_id = str(row.get("image_id", "")).strip()
                filename = row.get("filename", "").strip()
                if image_id and filename:
                    image_map[image_id] = filename
    except Exception as e:
        logging.error(f"Error loading image index {index_path}: {e}")
    return image_map

def load_category_map(json_path):
    category_map = {}
    try:
        with open(json_path, "r", encoding="utf-8-sig") as f:
            data = json.load(f)
        for entry in data.get("value", []):
            for media in entry.get("Media", []):
                category = str(media.get("Category", "")).strip()
                for file in media.get("MediaFiles", []):
                    fname = file.get("FileName", "").strip().lower()
                    if fname:
                        category_map[fname] = category
    except Exception as e:
        logging.error(f"Error loading category JSON {json_path}: {e}")
    return category_map

def fetch_all_data(backups, category_map):
    combined = {k: [] for k in QUERIES}
    for db_path, csv_path, image_dir in backups:
        label = os.path.basename(db_path)
        image_index = load_image_index(csv_path)
        try:
            conn = sqlite3.connect(db_path)
            for section, query in QUERIES.items():
                df = pd.read_sql_query(query, conn)
                df["source"] = label
                df["image_path"] = df["image_id"].apply(
                    lambda iid: os.path.join(os.path.basename(image_dir), image_index.get(str(iid).strip(), ""))
                    if pd.notna(iid) else ""
                )
                df["category"] = df["image_id"].apply(
                    lambda iid: category_map.get(image_index.get(str(iid).strip(), "").lower(), "")
                    if pd.notna(iid) else ""
                )
                combined[section].append(df)
            conn.close()
        except Exception as e:
            logging.error(f"Failed to query {db_path}: {e}")
    return {k: pd.concat(v, ignore_index=True) if v else pd.DataFrame() for k, v in combined.items()}

def render_html(data_frames, output_file):
    env = Environment()
    template = env.from_string("""
    <!DOCTYPE html><html><head><meta charset="UTF-8"><title>Kik Report</title>
    <style>
    body { font-family: Arial; margin: 20px; }
    .tabs { display: flex; margin-bottom: 10px; }
    .tab-button { margin-right: 10px; padding: 8px; background: #eee; border: 1px solid #ccc; cursor: pointer; }
    .tab { display: none; }
    .tab.active { display: block; }
    table { width: 100%; border-collapse: collapse; margin-bottom: 20px; }
    th, td { border: 1px solid #ccc; padding: 8px; text-align: left; }
    th { background: #f9f9f9; }
    img { max-width: 150px; height: auto; display: block; }
    </style><script>
    function showTab(id) {
        document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
        document.getElementById(id).classList.add('active');
    }
    </script></head><body>
    <h1>Kik Combined Report</h1>
    {% if sections %}
    <div class="tabs">{% for section in sections %}
    <div class="tab-button" onclick="showTab('{{ section.id }}')">{{ section.name }}</div>
    {% endfor %}</div>
    {% for section in sections %}
    <div class="tab" id="{{ section.id }}">
        <h2>{{ section.name }}</h2>
        <table><tr>{% for col in section.columns %}<th>{{ col }}</th>{% endfor %}<th>Image</th></tr>
        {% for row in section.records %}
        <tr>
            {% for col in section.columns %}
            <td>{{ row[col] }}</td>
            {% endfor %}
            <td>{% if row.image_path %}<img src="{{ row.image_path }}">{% endif %}</td>
        </tr>
        {% endfor %}</table></div>
    {% endfor %}
    <script>showTab('{{ sections[0].id }}');</script>
    {% else %}
    <p>No data available.</p>
    {% endif %}
    </body></html>
    """)
    sections = []
    for name, df in data_frames.items():
        if not df.empty:
            records = df.to_dict(orient="records")
            sections.append({
                "id": name.lower().replace(" ", "_"),
                "name": name,
                "columns": df.columns.tolist(),
                "records": records
            })
    with open(output_file, "w", encoding="utf-8") as f:
        f.write(template.render(sections=sections))
    logging.info(f"HTML report saved to: {output_file}")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("folder", help="Folder containing .backup, .csv, .json and image folders")
    args = parser.parse_args()

    backups, json_path = scan_folder(args.folder)
    if not backups:
        logging.error("No valid database/image index pairs found.")
        sys.exit(1)

    category_map = load_category_map(json_path)
    combined = fetch_all_data(backups, category_map)

    # 🟢 Output to the same directory as the first .backup file
    first_db_path = backups[0][0]
    db_dir = os.path.dirname(first_db_path)
    output_path = os.path.join(db_dir, "Combined_Kik_Report.html")

    render_html(combined, output_path)


if __name__ == "__main__":
    main()
