# smc_parking_citation

Collects Seattle Municipal Court parking citations from the public court records API
(`courtrecords-api.seattle.gov`), reads the infraction location from each ticket image, and geocodes it to
a census tract, to study whether parking enforcement differs across Seattle neighborhoods.

## Pipeline (`parking_pipeline.py`)

| Stage | What it does |
|---|---|
| `search` | Pages through the citation search one day at a time (days in parallel) and records parking citations: 9-digit citation numbers in the Infraction category, excluding Seattle Animal Control. |
| `fetch` | Finds each citation's ticket PDF and keeps only the page-1 ticket image. With `--ocr`, OCRs it immediately. |
| `ocr` | Tesseract OCR and parsing of violation, SMC section, fine, location, plate state, officer ID and comments. Handles current tickets and pre-2021 legacy receipts; scanned handwritten tickets are flagged `ticket_format = handwritten` for manual entry (`doc_link_uuid` lets them be re-downloaded). |
| `geocode` | U.S. Census batch geocoder: lat/lon and census tract per unique location. |
| `export` | Flat CSV for analysis. |
| `status` | Counts by stage. |

All stages are resumable; state lives in `data/parking.db` (SQLite). Requests from all workers share one
rate limit (`--interval`, default 3 per second) that halves on any error and recovers after clean runs.

## Running on GitHub Actions

`.github/workflows/pipeline.yml` runs the whole 2020-01-01 .. 2026-10-31 range in ~5-hour chunks on a
single runner, carrying the database between runs as the `state` artifact and queuing the next run until
the work is done. Start it from the Actions tab (**parking-pipeline → Run workflow**) or:

```powershell
gh workflow run pipeline.yml -R llayman12/smc_parking_citation
```

Get the latest results:

```powershell
$id = gh api repos/llayman12/smc_parking_citation/actions/artifacts?name=state"&"per_page=1 -q '.artifacts[0].workflow_run.id'
gh run download $id -n state -D C:\SMCData\ci -R llayman12/smc_parking_citation
python -c "import gzip,shutil; shutil.copyfileobj(gzip.open(r'C:\SMCData\ci\parking.db.gz'), open(r'C:\SMCData\ci\parking.db','wb'))"
python parking_pipeline.py --data-dir C:\SMCData\ci export --out C:\SMCData\ci\parking_citations.csv
```

When the run reports that all work is done, run it once more with **retry** checked, then disable the workflow.

## Running locally

```powershell
python -m pip install -r requirements.txt   # plus Tesseract: winget install UB-Mannheim.TesseractOCR
.\run_years.ps1                              # year by year into C:\SMCData
```
