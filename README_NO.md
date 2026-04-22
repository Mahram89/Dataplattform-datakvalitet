# Data Quality Pipeline (CSV → Parquet)
### Utvikler Mahram S.

Dette prosjektet er en enkel Python-basert løsning for å lese inn CSV-data, validere innholdet, filtrere ugyldige rader og lagre resultatet i et mer analysevennlig format (Parquet).

Løsningen viser hvordan man kan håndtere datakvalitet på en strukturert måte, med tydelig logging og rapportering.

---

## Hva programmet gjør

Programmet:

- Leser inn en CSV-fil
- Sjekker at nødvendige kolonner finnes
- Validerer innholdet i hver rad
- Skiller ut ugyldige rader
- Lagrer gyldige data som Parquet
- Lagrer ugyldige rader som CSV
- Genererer en JSON-rapport med statistikk

---

## Påkrevde kolonner

Følgende kolonner må finnes i inputfilen:

- person_id
- name
- age
- city

---

## Valideringsregler

En rad regnes som ugyldig hvis:

- person_id mangler
- name mangler
- city mangler
- age mangler eller ikke er et tall
- age er mindre enn 0
- age er større enn 120

---

## Teknologier

- Python
- Polars
- Parquet (via pyarrow)

---

## Hvorfor Parquet

Parquet er et kolonnebasert filformat som gir:

- Raskere analyse
- Mindre lagring
- Bedre ytelse på store datasett

---

## Prosjektstruktur

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

## Hvordan kjøre prosjektet

### 1. Klon repo (valgfritt)

```bash
git clone <repo-url>
cd <repo-navn>
```

---

### 2. Opprett virtuelt miljø

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

### 3. Installer avhengigheter

```bash
pip install -r requirements.txt
```

---

### 4. Kjør programmet

```bash
python main.py --input people.csv --output-dir output
```

---

## Output

Programmet lager følgende filer i `output/`:

* people.parquet → gyldige data
* people_invalid.csv → ugyldige rader
* people_report.json → statistikk

---

## Eksempel på rapport

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

## Viktige konsepter i koden

* Path brukes til å håndtere filstier
* dict brukes til strukturert data
* tuple brukes til å returnere flere verdier
* logging brukes for sporbarhet
* argparse brukes for input fra terminal

---

## Mulige forbedringer

* Automatisk kjøring via scheduler eller cron
* Støtte for flere filer
* Bedre logging og feilhåndtering
* Integrasjon med skylagring
* CI/CD pipeline

---

## Kort prosjekt forklaring

Dette er en enkel batch-prosess som leser CSV-data, validerer datakvalitet, filtrerer ugyldige rader og lagrer resultatet som Parquet. Løsningen fokuserer på struktur, sporbarhet og analysevennlig lagring.

```bash
Markdown Preview.

Snarvei på min pc vs code:
Ctrl + Shift + V
```
