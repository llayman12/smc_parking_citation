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

## Shards

The 2020-01-01 .. 2026-10-31 range is split by day into 4 shards (`--shard K/4` takes days whose ordinal
% 4 == K), each with its own database and its own 5 requests/second budget:

| Shard | Runs on |
|---|---|
| 0, 1 | GitHub Actions (`.github/workflows/pipeline.yml`) |
| 2 | local PC: `.\run_shard.ps1 -Shard 2` |
| 3 | UW machine: `.\run_shard.ps1 -Shard 3 -DataDir C:\Temp\smc -BackupDir "$env:OneDrive\SMCData"` |

Each shard also backs off on its own when the court server returns errors or slows down.

### GitHub Actions

Each shard job runs ~5 hours, saves its database as the `state-shard-K-of-4` artifact and queues its own
next run until its work is done. Start or restart from the Actions tab or:

```powershell
gh workflow run pipeline.yml -R llayman12/smc_parking_citation -f shards="[0,1]"
```

Download a shard's latest database:

```powershell
$k = 0
$id = gh api "repos/llayman12/smc_parking_citation/actions/artifacts?name=state-shard-$k-of-4&per_page=1" -q '.artifacts[0].workflow_run.id'
gh run download $id -n "state-shard-$k-of-4" -D C:\SMCData\gh$k -R llayman12/smc_parking_citation
python -c "import gzip,shutil; shutil.copyfileobj(gzip.open(r'C:\SMCData\gh$k\parking.db.gz'), open(r'C:\SMCData\gh$k\parking.db','wb'))"
```

### Combine and export

```powershell
python parking_pipeline.py --data-dir C:\SMCData\all merge C:\SMCData\gh0\parking.db C:\SMCData\gh1\parking.db C:\SMCData\shard-2-of-4\parking.db C:\SMCData\uw\parking-shard-3-of-4.db
python parking_pipeline.py --data-dir C:\SMCData\all export --out C:\SMCData\parking_citations.csv
```

When a shard reports it is done, run it once more with retry (`-f retry=true`, or
`fetch --retry --ocr --delete-images all` locally).

## Setup

```powershell
python -m pip install -r requirements.txt   # plus Tesseract: winget install UB-Mannheim.TesseractOCR
```

## Known data gaps

Some cases have no ticket document at all (e.g. many January 2024 cases have only officer comment/notes
docket entries). They are kept with `status = no_doc`; the `error` column lists what documents the case had.
