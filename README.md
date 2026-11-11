# smc_parking_citation

Collects every Seattle Municipal Court (SMC) **parking citation** from January 2020 through October 2026 from
the court's public records API, reads the **infraction location** off each ticket image with OCR, and
geocodes it to a **census tract**. The goal is to test whether parking enforcement in Seattle differs
across demographically distinct neighborhoods.

The court's search API returns citations but not where they were written; the location exists only on
the ticket itself, a PDF attached to each case. So the pipeline has to download one document per ticket,
which is what makes this a multi-day, multi-machine job.

---

## Contents

1. [Quick start](#quick-start)
2. [Repository layout](#repository-layout)
3. [How the pipeline works](#how-the-pipeline-works)
4. [The data source](#the-data-source)
5. [Ticket formats](#ticket-formats)
6. [Database schema](#database-schema)
7. [Exported CSV](#exported-csv)
8. [Running at scale: shards](#running-at-scale-shards)
9. [Runner: GitHub Actions](#runner-github-actions-shards-0-and-1)
10. [Runner: Windows PC](#runner-windows-pc-shard-2)
11. [Runner: Linux host without root](#runner-linux-host-without-root-shard-3-ovid12)
12. [Combining shards and exporting](#combining-shards-and-exporting)
13. [Rate limiting and server courtesy](#rate-limiting-and-server-courtesy)
14. [Performance](#performance)
15. [Known data gaps and caveats](#known-data-gaps-and-caveats)
16. [Notes for the analysis](#notes-for-the-analysis)
17. [Command reference](#command-reference)
18. [Troubleshooting](#troubleshooting)

---

## Quick start

```powershell
python -m pip install -r requirements.txt
winget install UB-Mannheim.TesseractOCR          # Tesseract OCR (Windows)

# one week, end to end, into .\data
python parking_pipeline.py search --from 2025-11-03 --to 2025-11-09
python parking_pipeline.py fetch --ocr --delete-images
python parking_pipeline.py geocode
python parking_pipeline.py export --out parking_citations.csv
python parking_pipeline.py status
```

Every stage is resumable: rerunning skips days already searched and tickets already fetched.

---

## Repository layout

| Path | Purpose |
|---|---|
| `parking_pipeline.py` | The whole pipeline: search, fetch, OCR, geocode, export, plus shard tools (`remaining`, `backup`, `merge`). |
| `run_shard.ps1` | Runs one shard to completion on a Windows machine, in hourly chunks. |
| `linux/setup.sh` | Installs Python 3.12 + Tesseract with micromamba on a Linux host without root, outside the home quota. |
| `linux/run_shard.sh` | Bash equivalent of `run_shard.ps1`, with compressed hourly backups. |
| `.github/workflows/pipeline.yml` | Runs shards 0 and 1 on GitHub Actions in ~5-hour chunks that chain themselves. |
| `requirements.txt` | `requests`, `pypdf`, `pillow`, `pytesseract`. |
| `archive/` | Earlier prototype scripts and scrapes. Local only (git-ignored). |

---

## How the pipeline works

```
 search ──► citations table (status=new)
               │
 fetch  ──► find ticket PDF ─► download ─► keep page-1 image (status=fetched)
               │                              │
               │                         --ocr: OCR immediately
 ocr    ──► Tesseract ─► parse fields ─► (status=ocr, ticket_format=printed|handwritten)
               │
 geocode ─► Census batch geocoder ─► geocodes table (lat/lon, tract)
               │
 export ──► CSV (citations ⨝ geocodes)
```

| Stage | What it does | Requests per unit |
|---|---|---|
| `search` | For each day (Seattle local time) pages through the citation search, 500 per page, and records parking citations. A day is marked done only when every page is in and the count matches the server's total. Days run in parallel. | ~3–5 per day |
| `fetch` | Lists the case's documents, picks the ticket PDF, downloads it, keeps the largest image on page 1 as a grayscale PNG and discards the rest (vehicle photos, certification pages). With `--ocr` the image is OCR'd as soon as it lands; with `--delete-images` it is deleted afterwards. | 2 per ticket |
| `ocr` | Tesseract (`--psm 4`) on each image, then regex parsing of the printed fields. Tickets without a location label are flagged `handwritten`. Legacy 400-px-wide receipts are upscaled 2× first. | none (CPU only) |
| `geocode` | Sends each distinct location string to the U.S. Census batch geocoder (1,000 per request) and stores lon/lat, state/county/tract/block FIPS and the 11-digit tract GEOID. Handles intersections such as `3RD AVE & PINE ST`. | 1 per 1,000 locations |
| `export` | One row per OCR'd citation with its geocode joined on. | none |

Statuses in the `citations` table:

| `status` | Meaning |
|---|---|
| `new` | Found by search, not yet fetched. |
| `fetched` | Image downloaded, not yet OCR'd. |
| `ocr` | OCR'd and parsed. Final state for usable tickets. |
| `no_doc` | The case has no ticket PDF. `error` lists the documents it did have (for auditing). |
| `error` | A request or PDF problem. Retried with `fetch --retry`. |

---

## The data source

Seattle Municipal Court records API: `https://courtrecords-api.seattle.gov`
Court ID: `68f021c4-6a44-4735-9a76-5360b2e8af13`

| Endpoint | Used for |
|---|---|
| `GET /courts/cms/citations?violationDateFrom&violationDateTo&caseHeader.courtID&page&size&sort` | Day-by-day citation search. |
| `GET /courts/cms/docketentrydocumentsaccess?caseHeader.courtID&caseHeader.caseInstanceUUID` | Every document on one case (no docket-entry ID needed). |
| `GET /courts/{court}/cms/case/{caseUUID}/docketentrydocuments/{documentLinkUUID}` | Download one document (PDF). |
| `GET /courts/{court}/cms/cases/{caseUUID}` and `/charges` | Case classification and disposition (optional, `fetch --with-charges`). |

Findings that shaped the design:

- **The search ignores classification filters.** Parking is identified from the result itself: a
  **9-digit, all-numeric citation number** in the `Infraction` category. 13-digit numbers are traffic-camera
  tickets; `9A9999999`-style numbers are officer traffic tickets. Sampling case headers confirmed 12 of 12
  9-digit infractions were classified `Infraction - Parking & Traffic Camera - Parking`.
- **9-digit `129…` numbers from Seattle Animal Control are not parking** (classified
  `Infraction - Traffic - MCIS Legacy`) and are excluded by `citing_agency`.
- **Do not filter on `closedFlag`.** About 70% of cases are closed; filtering to open cases loses most tickets.
- **Use Seattle local time with the right UTC offset.** Day boundaries are built with
  `America/Los_Angeles`, so PST/PDT changes are handled. A fixed `-07:00` shifts winter days by an hour.
- **Search results include the citing agency** (`citingAgencyActorInstance`), stored as `citing_agency`.
- **No rate-limit headers** are returned. Document downloads take the server several seconds each, so
  document requests are the costly part.
- **Range requests don't help.** The server honours byte ranges, but every request pays the same
  multi-second server cost, so fetching only page 1 in pieces was 5–10× slower than one full download.
- The court-wide document listing ignores filters and caps at 10,000 results, so it can't replace
  per-case lookups.

No names, addresses of defendants, or full plate numbers are written to the structured columns. Note,
however, that `ocr_text` holds the full OCR'd ticket text, which includes the plate, VIN and officer name.
All of it comes from public court records.

---

## Ticket formats

The ticket PDF is found by name, in this order:

| Era / type | Citation numbers | Document name | Page 1 | Location label | Handled as |
|---|---|---|---|---|---|
| Current handheld e-ticket | `74x…` (e.g. `745…`) | the citation number | 2240×1096 printed ticket image, ~2 MB PDF, 7 pages | `INFRACTION LOCATION:` | `printed` |
| Current scanned paper ticket | `13x…`, `17x…` (~0.5%) | `Complaint - Parking Citation` | scan of a handwritten form | — | `handwritten` |
| Legacy (migrated from MCIS, ~2020) e-ticket | `72x…` | `spd docs` | 400-px-wide printed receipt, ~450 KB PDF | `LOCATION:` | `printed` |
| Legacy scanned paper ticket | `173…` | `parkers` | 2552×3300 scan | — | `handwritten` |

Fields parsed from printed tickets: violation text, SMC section, fine, infraction location, plate
state (`State: WA` on current tickets, the suffix of `License: ABC1234-WA` on legacy ones), officer ID,
infraction date/time and public comments.

OCR clean-ups:
- **Merged street names** (`416 11THAVEE` → `416 11TH AVE E`) are repaired before geocoding.
- **Slashed zeros** in the legacy receipt font OCR as `@` (`11.70.02@`, `@7:19`) and are mapped back to `0`.
- `Public Comments` sometimes OCRs as `ublic Comments`; the parser allows for it.
- A blank `State:` field no longer falls through to "State of Washington".

**Handwritten tickets** (`ticket_format = handwritten`) can't be read by Tesseract. Their parsed fields
are left empty, and `doc_link_uuid` is kept so the image can be downloaded again for manual entry. They appear
to be issued by police officers and may be tied to incidents, so check whether they cluster in particular
areas before excluding them.

---

## Database schema

SQLite, one file per shard (`<data-dir>/parking.db`).

### `citations`

| Column | Source | Notes |
|---|---|---|
| `citation_number` | search | Primary key. |
| `case_uuid`, `case_number` | search | |
| `violation_date` | search | ISO timestamp in **UTC**, from the API. Exact date and time for every ticket. |
| `filed_date` | search | |
| `closed_at_search` | search | 1 if the case was closed when searched. |
| `citing_agency` | search | e.g. `Seattle Police Department`, `Seattle Department of Transportation`, `Seattle Parks Department`. |
| `status`, `error`, `updated_at` | pipeline | See statuses above. |
| `image_path` | fetch | Cleared when the image is deleted after OCR. |
| `doc_link_uuid` | fetch | The ticket PDF's ID, for re-downloading. |
| `case_classification`, `statute_number`, `statute_description`, `dispo_type`, `dispo_method` | fetch `--with-charges` | Optional; costs 2 more requests per ticket. |
| `ticket_format` | ocr | `printed` or `handwritten`. |
| `ocr_text` | ocr | Full OCR text (allows re-parsing without re-OCR: `ocr --reparse`). |
| `violation`, `smc`, `fine` | ocr | e.g. `11.72.465 \| CURB RAMP`, `11.72.465`, `47.0`. |
| `infraction_location` | ocr | Normalized upper-case street address or intersection. |
| `plate_state` | ocr | Two-letter state only. |
| `officer_id` | ocr | Numeric ID printed on the ticket. |
| `infraction_datetime` | ocr | As printed; incomplete on legacy tickets. Prefer `violation_date`. |
| `public_comments` | ocr | Officer's comment, e.g. `PAY TO PARK 8AM - 10PM ...`. |

### `geocodes`

`location` (PK, same string as `infraction_location`), `match` (`Match` / `No_Match` / `Tie`),
`match_type`, `matched_address`, `lon`, `lat`, `state_fips`, `county_fips`, `tract`, `block`,
`geoid_tract` (11-digit state+county+tract GEOID, joins directly to ACS tables).

### `searched_days`

`day`, `total` (all citations that day), `parking` (parking citations recorded). Useful for checking
coverage and daily volumes.

### `meta`

`shard` = the `K/N` the database belongs to; the pipeline refuses to mix shards.

---

## Exported CSV

`export` writes one row per citation with `status = 'ocr'`, ordered by `violation_date`:

`citation_number, citing_agency, ticket_format, case_number, violation_date, infraction_datetime,
filed_date, closed_at_search, violation, smc, fine, infraction_location, plate_state, officer_id,
public_comments, case_classification, statute_number, statute_description, dispo_type, dispo_method,
geocode_match, matched_address, lat, lon, geoid_tract`

`no_doc` and `error` rows are not exported. Count them with `status` or query the database directly.

---

## Running at scale: shards

About 2 million tickets need two requests each. To finish in days rather than weeks, the date range is
split into **4 shards by day**: shard `K` takes every day whose ordinal satisfies `day.toordinal() % 4 == K`.
Each shard searches and fetches only its own days, has its own database and its own rate limit, and never
touches another shard's tickets, so the machines need no coordination. Interleaving days (rather than
giving each machine a block of years) spreads every year across all four, so all years fill in together.

| Shard | Runs on | Database lives | Backup |
|---|---|---|---|
| 0 | GitHub Actions | runner disk | `state-shard-0-of-4` artifact (7-day retention, replaced every run) |
| 1 | GitHub Actions | runner disk | `state-shard-1-of-4` artifact |
| 2 | Windows PC | `C:\SMCData\shard-2-of-4` | optional `-BackupDir` |
| 3 | UW Linux host `ovid12` | `/var/tmp/smc-llayman/shard-3-of-4` | `~/smc_backup/parking-shard-3-of-4.db.gz`, hourly |

All shards cover **2020-01-01 to 2026-10-31**. To change the shard count, every shard must start over
with fresh databases (a database is locked to its `K/N`).

---

## Runner: GitHub Actions (shards 0 and 1)

`.github/workflows/pipeline.yml`:

- A matrix job per shard. **Per-shard concurrency** means at most one job per shard at any time.
- Each job: install Tesseract → **restore** the shard's latest `state-shard-K-of-4` artifact → search
  (stops starting new days at 260 min) → fetch with inline OCR (stops at 280 min) → geocode (stops at
  300 min) → write a status table to the run summary → **save** the database as a new artifact →
  **queue the shard's next run** if `remaining` > 0.
- The search/fetch step is `continue-on-error`, so progress is saved even if it fails. Saving is skipped
  if restore failed, so an empty database can never overwrite real progress.
- A schedule every 2 hours restarts any chain that broke.

```powershell
# start or restart
gh workflow run pipeline.yml -R llayman12/smc_parking_citation -f shards="[0,1]"
# one shard, at a lower rate
gh workflow run pipeline.yml -R llayman12/smc_parking_citation -f shards="[1]" -f interval=0.333
# final pass over errors and missing documents
gh workflow run pipeline.yml -R llayman12/smc_parking_citation -f shards="[0,1]" -f retry=true
# watch
gh run list -R llayman12/smc_parking_citation -L 5
```

Each run's summary page shows the shard's `status` output. When a shard reports it is done, run it once
with `retry=true`, then disable the workflow (Actions → parking-pipeline → ⋯ → Disable) so the backstop
schedule stops.

> The repository is public, so the state artifacts (including `ocr_text`) can be downloaded by any
> signed-in GitHub user. The source data is public court records.

---

## Runner: Windows PC (shard 2)

```powershell
.\run_shard.ps1 -Shard 2
```

Loops in 60-minute chunks: search → fetch with inline OCR and image deletion → geocode → optional backup
→ check `remaining`, until the shard is done. Safe to stop with Ctrl-C and rerun.

| Parameter | Default | |
|---|---|---|
| `-Shard` | (required) | |
| `-Of` | 4 | |
| `-DataDir` | `C:\SMCData\shard-K-of-4` | Keep it out of OneDrive: a sync client can corrupt a live SQLite file. |
| `-BackupDir` | none | e.g. `"$env:OneDrive\SMCData"`. Hourly consistent copy, written to a temp name then renamed. |
| `-Interval` | 0.2 | Seconds between requests (5/s). |
| `-ChunkMinutes` | 60 | |

The PC must stay awake (Settings → Power → Sleep: Never while plugged in).

---

## Runner: Linux host without root (shard 3, ovid12)

UW's `ovid12.u.washington.edu` is CentOS 7 with Python 3.8, no Tesseract, no root, an NFS home
directory and a **1 GB home quota**. The setup therefore keeps everything large on local disk in
`/var/tmp` (outside the quota and off NFS, where SQLite locking is unreliable):

| What | Where | Size |
|---|---|---|
| Code | `~/smc_parking_citation` (git clone) | < 1 MB |
| Python 3.12 + Tesseract 5.5 (micromamba) | `/var/tmp/smc-llayman/env` | ~650 MB |
| Working database | `/var/tmp/smc-llayman/shard-3-of-4/parking.db` | grows to several hundred MB |
| Hourly compressed backup | `~/smc_backup/parking-shard-3-of-4.db.gz` | ~¼ of the database |

```bash
# one-time (rerun if /var/tmp is ever wiped)
git clone https://github.com/llayman12/smc_parking_citation.git ~/smc_parking_citation
bash ~/smc_parking_citation/linux/setup.sh

# start in a detached tmux session (survives logout)
tmux new -d -s shard3 'bash ~/smc_parking_citation/linux/run_shard.sh 3'

# check on it
tmux attach -t shard3                  # Ctrl-B then D to detach
tail -f /var/tmp/smc-llayman/shard-3-of-4/run.log
```

`run_shard.sh` runs the pipeline at `nice 19` with 4 OCR processes to stay polite on a shared host. If the
working database is missing on start (e.g. `/var/tmp` was cleaned), it restores from the backup.
Environment overrides: `BASE`, `BACKUP_DIR`, `FROM`, `TO`, `INTERVAL`, `CHUNK_MIN`, `OCR_WORKERS`.

SSH from the Windows PC uses the `smc-runner` host alias in `~/.ssh/config` (key `~/.ssh/smc-runner`):

```powershell
ssh smc-runner
scp smc-runner:smc_backup/parking-shard-3-of-4.db.gz C:\SMCData\
```

---

## Combining shards and exporting

```powershell
# 1. GitHub shards: download each latest artifact
foreach ($k in 0, 1) {
  $id = gh api "repos/llayman12/smc_parking_citation/actions/artifacts?name=state-shard-$k-of-4&per_page=1" -q '.artifacts[0].workflow_run.id'
  gh run download $id -n "state-shard-$k-of-4" -D "C:\SMCData\gh$k" -R llayman12/smc_parking_citation
  python -c "import gzip,shutil,sys; shutil.copyfileobj(gzip.open(sys.argv[1]), open(sys.argv[2],'wb'))" "C:\SMCData\gh$k\parking.db.gz" "C:\SMCData\gh$k\parking.db"
}

# 2. ovid12 shard
scp smc-runner:smc_backup/parking-shard-3-of-4.db.gz C:\SMCData\uw\
python -c "import gzip,shutil,sys; shutil.copyfileobj(gzip.open(sys.argv[1]), open(sys.argv[2],'wb'))" C:\SMCData\uw\parking-shard-3-of-4.db.gz C:\SMCData\uw\parking.db

# 3. merge and export
python parking_pipeline.py --data-dir C:\SMCData\all merge C:\SMCData\gh0\parking.db C:\SMCData\gh1\parking.db C:\SMCData\shard-2-of-4\parking.db C:\SMCData\uw\parking.db
python parking_pipeline.py --data-dir C:\SMCData\all export --out C:\SMCData\parking_citations.csv
```

`merge` can be run at any time on partial shards to look at progress; rerunning it with newer copies
replaces older rows.

---

## Rate limiting and server courtesy

All requests from one process go through a shared `Client`:

- **Spacing.** Request *starts* are at least `--interval` seconds apart across all worker threads
  (default 0.2 s = 5 requests/second per shard), however many workers run.
- **Errors halve the rate.** Any HTTP 429, 5xx, timeout or connection error doubles the interval (up to
  10 s) and pauses every worker for 5 s, 15 s, 45 s, 135 s on successive attempts (4 retries).
- **Slow responses also cut the rate.** If the smoothed response time rises above 10 s, the interval
  grows 1.5× (at most once a minute) before the server starts failing.
- **Recovery.** After 200 clean responses in a row (and with latency back below 5 s), the rate rises 10% at a
  time back to `--interval`.
- Fetch prints the current rate, average latency and failure count every 100 tickets. Rising latency is the
  signal to lower `--interval` or `--workers`.

With four shards the court sees up to ~20 requests/second in total. The API's published notes place no
restrictions on automated access to this kind of search, and no rate-limit headers or 429 responses have
been observed.

---

## Performance

Measured, per shard at 5 requests/second:

| Stage | Throughput |
|---|---|
| Search | ~19 days/minute (a full year in ~20 min) |
| Fetch | up to 9,000 tickets/hour (2 requests each); ~5,400/hour observed at 3 req/s |
| OCR | ~1.3 CPU-seconds per ticket with one Tesseract thread per process → ~11,000/hour on 4 cores |

Tesseract is limited to one thread per process (`OMP_THREAD_LIMIT=1`). Running many processes that each
start their own thread pool oversubscribed the CPU: 6.3 vs 1.3 CPU-seconds per ticket.

Volume: 279,378 parking citations in 2020; weekdays typically see 1,000–1,600. Roughly 2 million tickets
for 2020–October 2026, about 3–4 days of wall time across four shards.

---

## Known data gaps and caveats

- **Cases with no ticket document.** Some cases have no ticket PDF at all. In a January 2024 sample, 27 of
  28 cases had only `Declaration - Officers Comments` / `Officers Notes` docket entries (comment text, no
  location). June 2024 was complete. These are kept as `no_doc`. Measure the gap by month before analysis:
  ```sql
  SELECT substr(violation_date,1,7) AS month, status, COUNT(*) FROM citations GROUP BY 1, 2;
  ```
- **Geocoding misses.** About 2–3% of distinct printed locations returned `No_Match` from the Census
  geocoder even though they look valid (e.g. `1222 HARBOR AVE SW`). A second geocoder could recover them.
- **Handwritten tickets** (~0.5%) have no parsed fields and need manual entry. They may not be a random subset.
- **OCR errors** remain in some fields (e.g. `11.76.0015` for `11.76.015`). `violation` and `smc` carry the
  same section, so they can cross-check each other. `infraction_location` is checked by the geocoder.
- **`infraction_datetime`** is incomplete on legacy tickets. Use `violation_date` (UTC) and convert to
  Seattle time.
- **Snapshot in time.** `closed_at_search` and late-filed documents reflect when the shard searched.
  Running `fetch --retry` later can pick up documents filed after the first pass.

---

## Notes for the analysis

- **Complaint-driven vs patrol-driven.** Public comments such as `CSR COMPLAINT` mark tickets written in
  response to a resident complaint. Those measure resident behaviour as much as enforcement, so analyse them
  separately.
- **Denominators.** Raw counts by tract mostly reflect where parking and parking rules exist. Normalise by
  exposure: SDOT curb-space, paid-parking and restricted parking zone (RPZ) data, blockface counts, or
  on-street parking supply.
- **Demographics.** Join `geoid_tract` to American Community Survey (ACS) 5-year tract tables.
- **Ticket mix.** Compare violation types (`smc`), fines and officers (`officer_id`) across tracts, not just
  totals.
- **Time.** Volumes changed sharply in 2020 (COVID-era enforcement changes are visible in `searched_days`).
  Check `citing_agency` and daily volumes over time before pooling years.
- If the work is published through UW, check with the UW Human Subjects Division (IRB) about whether review
  is needed.

---

## Command reference

Global options (before the subcommand):

| Option | Default | |
|---|---|---|
| `--data-dir` | `data` | Folder holding `parking.db` and `tickets/`. |
| `--interval` | 0.2 | Minimum seconds between requests (5/s). Adapts automatically. |
| `--shard K/N` | `0/1` | Only days with ordinal % N == K. |
| `--stop-at` | none | Unix time after which search/fetch/geocode start no new work. |

| Command | Options |
|---|---|
| `search --from YYYY-MM-DD --to YYYY-MM-DD` | `--workers 4` (days in parallel), `--sample 0.05` (deterministic fraction of tickets), `--redo` |
| `fetch` | `--workers 16`, `--ocr`, `--ocr-workers N`, `--delete-images [printed\|all]`, `--retry`, `--limit N`, `--with-charges` |
| `ocr` | `--workers N`, `--delete-images [printed\|all]`, `--redo`, `--reparse` (re-parse stored text only) |
| `geocode` | `--batch 1000` |
| `export` | `--out FILE.csv` |
| `status` | Counts by status, days searched, top errors. |
| `remaining --from --to` | Prints days not searched + tickets not finished for this shard (0 = done). |
| `backup --to FILE.db` | Consistent copy while running (temp file + rename). |
| `merge SHARD.db ...` | Combine shard databases into `--data-dir`. |

---

## Troubleshooting

| Symptom | Cause / fix |
|---|---|
| `holds shard 2/4, not 3/4` | Each shard needs its own `--data-dir`. |
| `Tesseract OCR not found` | Install it, or set `TESSERACT_CMD` to the executable. |
| Many `no_doc` with `pdfs: [...]` listing unfamiliar names | A ticket document name the pipeline doesn't know yet. Add it to the name list in `fetch_one`. |
| `saw X of Y results` on a search day | New filings arrived mid-search. The day isn't marked done; the next run retries it. |
| Rate keeps dropping (`rate cut to ...`) | The server is erroring or slow. Leave it; it recovers on its own. Lower `--interval` for good if it persists. |
| GitHub shard stopped chaining | Restart it with `gh workflow run ... -f shards="[K]"`. The 2-hour schedule also restarts it. |
| `database is locked` | Two processes on one database. Run one pipeline process per data directory. |
| ovid12 tmux session gone after a reboot | Rerun `linux/setup.sh` if `/var/tmp` was wiped, then start the tmux session again. It restores from `~/smc_backup`. |
| Key login to `smc-runner` asks for a passphrase | The key has a passphrase. Remove it: `ssh-keygen -p -f ~/.ssh/smc-runner -N ''`. |
