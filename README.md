# Blue Kik Parser

## Overview
Blue Kik Parser is a Python-based tool designed to extract messages, images, and content metadata from Blue Kik Messenger SQLite database backups. It supports batch processing and structured data export to Excel files for analysis.

## Features
- Extracts private messages, group messages, and image metadata.
- Uses `was_me` to determine the sender (1 = Account Owner, 0 = Other User).
- Converts UNIX timestamps to human-readable format.
- Processes large databases in chunks to optimize performance.
- Logs detailed execution steps and errors for debugging.

## Requirements
- Python 3.x
- Required Python libraries:
  - `sqlite3`
  - `pandas`
  - `openpyxl`
  - `argparse`
  - `logging`
  - `os`

## Installation
1. Clone the repository:
   ```sh
   git clone https://github.com/marcuspenn4n6/Blue-Kik-Parser/blue-kik-parser.git
   cd blue-kik-parser
   ```
2. Install dependencies:
   ```sh
   pip install pandas openpyxl
   ```

## Usage
### Extracting Images
To extract image BLOBs from the database:
```sh
python BlueKikImageBlobExtractor.py path/to/database
```
Images will be saved in a directory named:
```sh
<database name>_images/
```

### Parsing Messages
Run the script with the path to the SQLite database:
```sh
python BlueKikParser.py path/to/database
```
This will generate an Excel file:
```sh
Blue Kik Parsed - <database name>.xlsx
```
with three sheets:
- **Private Messages**
- **Group Messages**
- **Images & Content**

## Database Schema
The script processes the following tables:
- `messagesTable` (stores messages with `partner_jid`, `was_me`, `body`, `timestamp`)
- `memberTable` (stores group member associations)
- `KIKContentTable`, `KIKContentURITable` (stores image metadata)
- `AccountSwitcherImgBackupTable` (stores image BLOBs)

For full schema details, see `DatabaseDefinitions.pdf`.

## Logging and Debugging
- The script logs all steps in `INFO` mode.
- Errors and missing fields are logged to help troubleshooting.

## License
This project is licensed under the MIT License.

## Contributors
- Marcus Penn (@marcuspenn4n6)

## Issues
For bug reports and feature requests, open an issue on GitHub.

---

*Note: Ensure you comply with data privacy laws before processing Blue Kik Messenger data.*
