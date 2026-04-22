# Data Quality Pipeline (CSV → Parquet)
### Developer: Mahram S.


This project is a simple Python-based solution for reading CSV data, validating its content, filtering invalid rows, and storing the result in a more analysis-friendly format (Parquet).

The solution demonstrates how to handle data quality in a structured way, with clear logging and reporting.

---

## What the program does

The program:

- Reads a CSV file
- Checks that required columns exist
- Validates each row
- Filters out invalid rows
- Stores valid data as Parquet
- Stores invalid rows as CSV
- Generates a JSON report with statistics

---

## Required columns

The following columns must exist in the input file:

- person_id
- name
- age
- city

---

## Validation rules

A row is considered invalid if:

- person_id is missing
- name is missing
- city is missing
- age is missing or not a number
- age < 0
- age > 120

---

## Technologies

- Python
- Polars
- Parquet (via pyarrow)

---

## Why Parquet

Parquet is a column-based file format that provides:

- Faster data analysis
- Reduced storage usage
- Better performance for large datasets

---

## Project structure

```text
.
├─ main.py
├─ requirements.txt
├─ README.md
├─ .gitignore
├─ people.csv
└─ output/
````

---

## How to run the project

### 1. Clone the repository (optional)

```bash
git clone <repo-url>
cd <repo-name>
```

---

### 2. Create a virtual environment

#### Windows (PowerShell)

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
```

#### Windows (CMD)

```cmd
python -m venv .venv
.venv\Scripts\activate.bat
```

#### Linux / macOS (bash)

```bash
python3 -m venv .venv
source .venv/bin/activate
```

---

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

---

### 4. Run the program

```bash
python main.py --input people.csv --output-dir output
```

---

## Output

The program generates the following files in the `output/` directory:

* `people.parquet` → valid data
* `people_invalid.csv` → invalid rows
* `people_report.json` → statistics

---

## Example report

```json
{
  "input_file": "people.csv",
  "total_rows": 8,
  "valid_rows": 4,
  "invalid_rows": 4,
  "output_parquet": "output/people.parquet",
  "invalid_rows_file": "output/people_invalid.csv"
}
```

---

## Key concepts in the code

* Path → used for handling file paths
* dict → used for structured data (report)
* tuple → used to return multiple values
* logging → used for tracking execution
* argparse → used for reading command line input

---

## Possible improvements

* Automated scheduling (cron or scheduler)
* Support for multiple files
* Improved logging and error handling
* Cloud storage integration
* CI/CD pipeline

---

## Short project explanation

This is a simple batch process that reads CSV data, validates data quality, filters invalid rows, and stores the result as Parquet for further analysis. The solution focuses on structure, traceability, and efficient data storage.

```bash
Markdown Preview.

Shortcut on my PC in VS Code:
Ctrl + Shift + V
```
