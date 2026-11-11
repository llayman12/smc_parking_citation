# smc_parking_citation

Collects every Seattle Municipal Court (SMC) **parking citation** from January 2020 through October 2026 from
the court's public records API, reads the **infraction location** off each ticket image with OCR, and
geocodes it to a **census tract**. The goal is to test whether parking enforcement in Seattle differs
across demographically distinct neighborhoods.

The court's search API returns citations but not where they were written; the location exists only on
the ticket itself, a PDF attached to each case. So the pipeline downloads one document per ticket, about
2 million of them, which is what makes this a multi-day job.

---

## Contents

1. [Quick start](#quick-start)
2. [Repository layout](#repository-layout)
3. [Architecture](#architecture)
4. [Running it on GitHub Actions](#running-it-on-github-actions)
5. [Getting the results: sync, merge, export](#getting-the-results-sync-merge-export)
6. [Ticket images](#ticket-images)
7. [The data source](#the-data-source)
8. [Ticket formats](#ticket-formats)
9. [Database schema](#database-schema)
10. [Exported CSV](#exported-csv)
11. [Rate limiting and server courtesy](#rate-limiting-and-server-courtesy)
12. [API limits: research and observations](#api-limits-research-and-observations)
13. [Performance](#performance)
14. [Known data gaps and caveats](#known-data-gaps-and-caveats)
15. [Notes for the analysis](#notes-for-the-analysis)
16. [Design history](#design-history)
17. [Ideas not yet built](#ideas-not-yet-built)
18. [Command reference](#command-reference)
19. [Troubleshooting](#troubleshooting)

---

## Quick start

The full collection runs on GitHub Actions:

```powershell
gh workflow run index.yml -R llayman12/smc_parking_citation     # build the index, then start the buckets
.\sync_results.ps1 -Merge                                       # any time: pull results, merge, export CSV
```

A small local run, end to end (Windows):

```powershell
python -m pip install -r requirements.txt
winget install UB-Mannheim.TesseractOCR

python parking_pipeline.py search    --from 2025-11-03 --to 2025-11-09      # citations, by violation day
python parking_pipeline.py list-docs --from 2025-11-03 --to 2025-11-20      # ticket documents, by filing day
python parking_pipeline.py build-index --buckets 16
python parking_pipeline.py --data-dir b0 take-bucket --index data\parking.db --bucket 0
python parking_pipeline.py --data-dir b0 fetch --ocr
python parking_pipeline.py --data-dir b0 geocode
python parking_pipeline.py --data-dir b0 export --out parking_citations.csv
```

Every stage is resumable: rerunning skips days already searched and tickets already fetched.

---

## Repository layout

| Path | Purpose |
|---|---|
| `parking_pipeline.py` | The whole pipeline: `search`, `list-docs`, `build-index`, `take-bucket`, `fetch`, `ocr`, `geocode`, `export`, plus `merge`, `status`, `remaining` and other helpers. |
| `.github/workflows/index.yml` | Step 1: builds the index (all citations joined to their ticket documents, split into 256 buckets). |
| `.github/workflows/buckets.yml` | Step 2: downloads and OCRs the tickets, one bucket per job, several jobs at a time. |
| `sync_results.ps1` | Pulls finished bucket databases and ticket images from GitHub to the PC and OneDrive; optionally merges, geocodes and exports. |
| `requirements.txt` | `requests`, `pypdf`, `pillow`, `pytesseract`. |
| `archive/` | Earlier prototype scripts and scrapes. Local only (git-ignored). |

---

## Architecture

```
index.yml                                   buckets.yml                          your PC
─────────                                   ───────────                          ───────
search     citations by violation day ─┐    plan: which buckets have no          sync_results.ps1
list-docs  ticket PDFs by filing day ──┤          "done-NNN" artifact yet?         ├─ done-NNN  → C:\SMCData\buckets
merge + build-index                    │    work (N jobs at a time):               │            → OneDrive\SMCData\db
   join on case, assign buckets ───────┴─►    take-bucket → fetch + OCR            ├─ images-NNN → OneDrive\SMCData\images
   "index" artifact                           upload done-NNN + images-NNN ──────► └─ -Merge: merge → geocode → export CSV
```

### The index

Two cheap listings, each a few requests per day of data, describe all the work before any ticket is downloaded:

| Listing | Endpoint | Keyed by | Gives |
|---|---|---|---|
| `search` | `/courts/cms/citations` | violation day | citation number, case, violation time, citing agency |
| `list-docs` | `/courts/cms/docketentrydocuments`, filtered to docket entry sub type `1001507` ("Complaint - Parking Citation") | docket-entry filing day | the ticket PDF's ID (`documentLinkUUID`) and its case |

`build-index` joins them on the case. The result is one row per citation with the ID of its ticket PDF:

- **One request per ticket.** `fetch` goes straight to the download. (The first design looked up each
  case's documents first: two requests per ticket.)
- **Missing documents are known up front.** A citation whose case has no parking-citation document is
  marked `no_doc` in the index and never requested. See [Known data gaps](#known-data-gaps-and-caveats).
- Documents are listed through *today*, not just through the last violation day, because a ticket's
  document can be filed days after the violation.

### Buckets: every unit is a random sample

`build-index` assigns each citation to one of **256 buckets** by a hash of its citation number
(`sha1(number) mod 256`). Consequences:

- Each bucket is a **uniform random sample** of the whole 2020–2026 range, about 0.4% of all tickets
  (roughly 9,000).
- **Any set of finished buckets is itself a uniform random sample.** After 26 buckets you hold a 10%
  sample of every month and every neighborhood, and analysis can start. Stopping early costs precision,
  not coverage.
- Inside a bucket, tickets are fetched in random order too (`ORDER BY case_uuid`; the IDs are random UUIDs).
- Buckets are independent. Any worker can do any bucket, in any order, with no coordination beyond
  "is there a `done-NNN` yet?".

### Workers

One GitHub job does one bucket: download the index, `take-bucket`, `fetch --ocr` (download each PDF, keep
the page-1 image, OCR it immediately), upload the bucket database and its images. A finished bucket is the
checkpoint, so at most one bucket's work is ever at risk. A job that runs out of time, or meets an
announced portal outage, uploads `part-NNN` instead; the next run resumes it.

### Statuses in the `citations` table

| `status` | Meaning |
|---|---|
| `new` | In the index with a ticket document, not yet fetched. |
| `fetched` | Image downloaded, not yet OCR'd (transient). |
| `ocr` | OCR'd and parsed. Final state for usable tickets. |
| `no_doc` | The case has no parking-citation document in the court's listing. |
| `error` | A request or PDF problem. Retried once at the end of the bucket (`fetch --retry-errors`). |

---

## Running it on GitHub Actions

### 1. Build the index (`index.yml`, ~45 minutes)

```powershell
gh workflow run index.yml -R llayman12/smc_parking_citation
```

Four jobs each search a quarter of the days (`--shard K/4`) and list the documents filed on them; each
repeats up to three passes until no day is missing, and fails if any still is. A merge job combines them,
runs `build-index --buckets 256 --slim`, writes a year-by-year table of citations with and without
documents to the run summary, uploads the **`index`** artifact (kept 90 days) and starts `buckets.yml`.

Inputs: `from`, `to` (violation days), `interval`, `start_buckets`.

### 2. Work the buckets (`buckets.yml`)

Started by the index run, by hand, or by its 6-hourly backstop schedule:

```powershell
gh workflow run buckets.yml -R llayman12/smc_parking_citation                 # 4 workers, 3.3 req/s each
gh workflow run buckets.yml -R llayman12/smc_parking_citation -f workers=6    # more sources of requests
gh workflow run buckets.yml -R llayman12/smc_parking_citation -f smoke=true   # 2 buckets x 40 tickets
```

- **plan** lists buckets with no `done-NNN` artifact newer than the current index (results from an older
  index don't count: its buckets were cut differently). During an announced portal outage it plans nothing.
- **work** runs one job per planned bucket, `workers` at a time. Only one run of this workflow is active at
  once; a second waits.
- Re-dispatching is always safe: it plans whatever is still missing. That is also how failed buckets are
  retried, and it is all the backstop schedule does.

| Input | Default | |
|---|---|---|
| `workers` | 4 | Bucket jobs at once. Each is a separate source of requests to the court. |
| `interval` | 0.3 | Seconds between requests per job. OCR on a 4-core runner is the slower stage, so a lower interval gains little. |
| `smoke` | false | 2 buckets, 40 tickets each, left as `part-NNN`. Exercises every step in about 5 minutes. |

Artifacts:

| Name | Contents | Kept |
|---|---|---|
| `index` | The index database (gzipped) | 90 days |
| `done-NNN` | A finished bucket's database (gzipped, ~2–3 MB) | 90 days |
| `part-NNN` | An unfinished bucket's database, to resume from | 90 days |
| `images-NNN` | The page-1 ticket images that job downloaded (~450 MB per bucket) | 30 days |
| `index-part-K` | Intermediate index pieces | 7 days |

GitHub is only a relay: **run `sync_results.ps1` at least every couple of weeks** while work is in progress,
or images will expire before they are saved.

Watch progress in the Actions tab (a running job's log streams live there), or:

```powershell
gh run list -R llayman12/smc_parking_citation -L 5
```

Each bucket job's summary shows its `status` counts.

> The repository is public, so artifacts (ticket images and OCR text, which include plates and officer
> names) can be downloaded by any signed-in GitHub user until they expire. The source is public court records.

---

## Getting the results: sync, merge, export

```powershell
.\sync_results.ps1            # download new bucket databases and image archives
.\sync_results.ps1 -Merge     # ...then merge all finished buckets, geocode, export the CSV
.\sync_results.ps1 -NoImages  # databases only
```

| What | Where |
|---|---|
| Bucket databases (working copies) | `C:\SMCData\buckets\done-NNN.db` |
| Bucket databases (compressed copies) | `<OneDrive - UW>\SMCData\db\done-NNN.db.gz` |
| Ticket images | `<OneDrive - UW>\SMCData\images\images-NNN-<id>.zip` |
| Merged database | `C:\SMCData\all\parking.db` |
| CSV | `C:\SMCData\parking_citations.csv` |

The script skips anything already downloaded, and ignores results older than the current index. `-Merge`
works on partial results and reports what fraction of the buckets (= what size random sample) it covers.
Geocoding happens here, once, over the merged set, rather than in every worker.

SQLite working files stay out of OneDrive on purpose: a sync client can corrupt a database that is being written.

---

## Ticket images

Every downloaded ticket's page-1 image is kept, for auditing OCR results and for entering handwritten
tickets by hand. Each is a grayscale PNG named `<citation number>.png` (current tickets ~55 KB, legacy
receipts ~40 KB): about **115 GB** for all ~2 million.

- GitHub can't hold them permanently (repository size limits; artifacts expire), so workers upload them as
  30-day `images-NNN` artifacts and `sync_results.ps1` moves them to OneDrive as one zip per bucket job.
  A few hundred large zips sync far better than millions of small files.
- Each zip is marked online-only as soon as it is written, so OneDrive uploads it and then frees the local
  disk. Check your OneDrive quota has ~115 GB free.
- To find a ticket's image: its bucket is `bucket` in the database (or `sha1(citation number)[8:16] mod 256`);
  open `images-NNN-*.zip` for that bucket.
- A QA sample (suggested: 200 random tickets) can be checked by hand against the parsed fields to get a
  real OCR error rate for the methods section.

---

## The data source

Seattle Municipal Court records API: `https://courtrecords-api.seattle.gov`
Court ID: `68f021c4-6a44-4735-9a76-5360b2e8af13`

| Endpoint | Used for |
|---|---|
| `GET /courts/cms/citations?violationDateFrom&violationDateTo&caseHeader.courtID&page&size&sort` | Day-by-day citation search. |
| `GET /courts/cms/docketentrydocuments?docketEntryFiledDateFrom&docketEntryFiledDateTo&docketEntrySubTypeID&caseHeader.courtID&page&size&sort` | Day-by-day listing of parking-citation documents. |
| `GET /courts/{court}/cms/case/{caseUUID}/docketentrydocuments/{documentLinkUUID}` | Download one document (PDF). |
| `GET /courts/cms/docketentrydocumentsaccess?caseHeader.caseInstanceUUID` | Every document on one case. Only used by `fetch --retry` for rows without a document ID. |
| `GET /courts/{court}/cms/cases/{caseUUID}` and `/charges` | Case classification and disposition (optional, `fetch --with-charges`). |

Findings that shaped the design:

- **The citation search ignores classification filters.** Parking is identified from the result itself: a
  **9-digit, all-numeric citation number** in the `Infraction` category. 13-digit numbers are traffic-camera
  tickets; `9A9999999`-style numbers are officer traffic tickets. Sampling case headers confirmed 12 of 12
  9-digit infractions were classified `Infraction - Parking & Traffic Camera - Parking`.
- **9-digit `129…` numbers from Seattle Animal Control are not parking** and are excluded by `citing_agency`.
- **The document listing does honour filters** on docket-entry filed date and sub type (but not on document
  name). Sub type `1001507` is "Complaint - Parking Citation" in every year tested, 2020–2025.
- **Paging needs an explicit, tie-free sort.** Without one, a 1,556-row listing came back with 10 rows
  repeated and 10 missing. The document listing sorts by `documentLinkUUID`; the citation search sorts by
  violation date *and* citation number (hundreds of tickets a day share a violation time).
- **The court's data contains a few true duplicate rows** (e.g. citation number `0` twice on one case). The
  completeness check therefore distinguishes a repeat within a page (real) from a repeat across pages
  (paging shifted).
- **Any listing is capped at 10,000 results**, so both listings go day by day; `list-docs` fails a day that
  reaches the cap rather than silently truncating it.
- **Do not filter on `closedFlag`.** About 70% of cases are closed.
- **Use Seattle local time with the right UTC offset** for day boundaries (`America/Los_Angeles`).
- **Range requests don't help.** The server honours byte ranges, but each request pays the same server
  cost, so fetching only page 1 in pieces was 5–10× slower than one full download.

The structured columns hold no names and no plate numbers. `ocr_text` and the images hold the full
ticket, which includes the plate, VIN and officer name. All of it comes from public court records.

---

## Ticket formats

All of these are filed under the "Complaint - Parking Citation" docket entry; they differ in document name
and layout:

| Era / type | Citation numbers | Document name | Page 1 | Location label | Handled as |
|---|---|---|---|---|---|
| Current handheld e-ticket (since ~March 2024) | `739…` and later | the citation number | 2240×1096 printed ticket image, ~2 MB PDF | `INFRACTION LOCATION:` | `printed` |
| Current scanned paper ticket | `13x…`, `17x…` (~0.5%) | `Complaint - Parking Citation` | scan of a handwritten form | — | `handwritten` |
| Legacy (migrated from MCIS, 2020–early 2024) e-ticket | `72x…`–`738…` | `spd docs` | 400-px-wide printed receipt, ~450 KB PDF | `LOCATION:` | `printed` |
| Legacy scanned paper ticket | `173…` | `parkers` | 2552×3300 scan | — | `handwritten` |

Fields parsed from printed tickets: violation text, SMC section, fine, infraction location, plate
state (`State: WA` on current tickets, the suffix of `License: ABC1234-WA` on legacy ones), officer ID,
infraction date/time and public comments.

OCR clean-ups:
- **Merged street names** (`416 11THAVEE` → `416 11TH AVE E`) are repaired before geocoding.
- **Slashed zeros** in the legacy receipt font OCR as `@` (`11.70.02@`, `@7:19`) and are mapped back to `0`.
- `Public Comments` sometimes OCRs as `ublic Comments`; the parser allows for it.
- A blank `State:` field no longer falls through to "State of Washington".
- Legacy receipts are upscaled 2× before OCR.

**Handwritten tickets** (`ticket_format = handwritten`) can't be read by Tesseract. Their parsed fields
are empty; enter them by hand from the saved images. They appear to be issued by police officers and may be
tied to incidents, so check whether they cluster in particular areas before excluding them.

---

## Database schema

SQLite. The index, each bucket and the merged result all use the same schema (`parking.db`).

### `citations`

| Column | Source | Notes |
|---|---|---|
| `citation_number` | search | Primary key. |
| `case_uuid`, `case_number` | search | |
| `violation_date` | search | ISO timestamp in **UTC**, from the API. Exact date and time for every ticket. |
| `filed_date` | search | |
| `closed_at_search` | search | 1 if the case was closed when searched. |
| `citing_agency` | search | e.g. `Seattle Police Department`, `Seattle Department of Transportation`, `Seattle Parks Department`. |
| `doc_link_uuid` | build-index | The ticket PDF's ID. NULL = no document (`no_doc`). |
| `bucket` | build-index | 0–255. |
| `status`, `error`, `updated_at` | pipeline | See statuses above. |
| `image_path` | fetch | Path on the machine that fetched it; the file name is `<citation_number>.png`. |
| `case_classification`, `statute_number`, `statute_description`, `dispo_type`, `dispo_method` | fetch `--with-charges` | Optional; 2 more requests per ticket. |
| `ticket_format` | ocr | `printed` or `handwritten`. |
| `ocr_text` | ocr | Full OCR text (allows re-parsing without re-OCR: `ocr --reparse`). |
| `violation`, `smc`, `fine` | ocr | e.g. `11.72.465 \| CURB RAMP`, `11.72.465`, `47.0`. |
| `infraction_location` | ocr | Normalized upper-case street address or intersection. |
| `plate_state` | ocr | Two-letter state only. |
| `officer_id` | ocr | Numeric ID printed on the ticket. |
| `infraction_datetime` | ocr | As printed; incomplete on legacy tickets. Prefer `violation_date`. |
| `public_comments` | ocr | Officer's comment, e.g. `PAY TO PARK 8AM - 10PM ...`. |

### Other tables

| Table | Contents |
|---|---|
| `geocodes` | `location` (same string as `infraction_location`), `match` (`Match` / `No_Match` / `Tie`), `match_type`, `matched_address`, `lon`, `lat`, `state_fips`, `county_fips`, `tract`, `block`, `geoid_tract` (11-digit GEOID, joins directly to ACS tables). |
| `searched_days` | `day`, `total` (all citations that day), `parking` (parking citations recorded). |
| `documents`, `listed_days` | The parking-citation document listing and which filing days are covered. Emptied from the published index (`--slim`); present in `index-part-K`. |
| `latency_log` | One row per minute of requests: `minute` (Seattle time), `requests`, `total_latency`, `max_latency`, `failures`, `rate_cap`. For measuring server capacity from real runs. |
| `meta` | `buckets`, `bucket`, `shard`. |

---

## Exported CSV

`export` writes one row per citation with `status = 'ocr'`, ordered by `violation_date`:

`citation_number, citing_agency, ticket_format, case_number, violation_date, infraction_datetime,
filed_date, closed_at_search, violation, smc, fine, infraction_location, plate_state, officer_id,
public_comments, case_classification, statute_number, statute_description, dispo_type, dispo_method,
geocode_match, matched_address, lat, lon, geoid_tract`

`no_doc` and `error` rows are not exported. Count them with `status` or query the database directly.

---

## Rate limiting and server courtesy

All requests from one process go through a shared `Client`:

- **Spacing.** Request *starts* are about `--interval` seconds apart across all worker threads, with ±20%
  random jitter so several machines don't fall into step.
- **Errors halve the rate.** Any HTTP 429, 5xx, timeout or connection error doubles the interval (up to
  10 s) and pauses every worker for 5 s, 15 s, 45 s, 135 s on successive attempts (4 retries).
- **Slow responses cut the rate.** If the smoothed response time rises above 6 s, the interval grows 1.5×
  (at most every 30 s), before the server starts failing.
- **Recovery in minutes.** After 30 clean responses in a row, with latency back under 3 s, the interval
  shrinks 15% toward `--interval`. (The first version needed 200 responses per 10% step: over an hour to
  recover from a deep cut.)
- **OCR backpressure.** Fetching pauses when 25 images per OCR process are waiting, so downloads never
  outrun OCR.
- **Announced outages.** `MAINTENANCE_WINDOWS` in `parking_pipeline.py` lists portal maintenance windows
  (Seattle time). No requests are made inside one: running jobs stop and save, and the planner plans
  nothing until it is over. Add new windows as the court announces them on its
  [public records page](https://seattle.gov/courts/programs-and-services/public-records-requests).
- **Empty-response check** (only for `fetch --retry`, which looks cases up one by one): a run of 15 "no
  documents at all" answers is rechecked before being believed.

With the defaults the court sees at most 4 sources × 3.3 requests/second, and usually less, because OCR is
the slower stage.

---

## API limits: research and observations

### What the documentation says (researched 2026-11-10)

- **Software.** The court's case management system is Thomson Reuters **C-Track** (with **cFive** for
  probation), which replaced the legacy MCIS system around March 2024 ([SMC technology page](https://seattle.gov/courts/about/technology)).
  Pre-2024 cases were migrated, which is why older tickets have different document names and layouts.
  The portal API is a Spring Boot service; it publishes an OpenAPI 3 description at `/v3/api-docs` titled
  "Public Portal APIs" (238 paths). That description is where the filterable document listing was found.
- **No published limits.** The OpenAPI description defines no security scheme and says nothing about rate
  limits, quotas or acceptable use. Responses carry no rate-limit headers. The portal's public
  configuration endpoints contain no usage terms. The admin `actuator` endpoints listed in the description
  were deliberately not queried.
- **Court policy.** The court's public records page says requests for "court data or bulk court records from
  the court's case management system" go through its Public Records Portal, and cites General Rule 31 and
  JISCR 15. It says nothing specific about automated use of the online portal.

### What has been observed

| When (Seattle time) | Load | What happened |
|---|---|---|
| Nov 4–10 tests | 1–5 req/s from one source | No errors, latency 0.3–4 s. |
| 2026-11-10 ~22:13 | ~10 req/s from one IP | 217 per-case document lookups in one minute came back `200 OK` with **empty lists**. |
| 2026-11-10 ~22:15–22:35 | ~20 req/s total from 4 sources | Latency rose from ~1 s to 10–40 s on two machines at once, with no HTTP errors; back to 0.3 s by 22:38. |

**These are one evening's events and prove little.** Both happened in the same 20 minutes, so they may be
one cause, and that cause may be the court's own nightly processing rather than this project's load. They
are not a measured ceiling.

### How limits are measured now

Deliberately ramping requests until the server fails was rejected: the failure mode seen so far is slow or
silently wrong answers on a public portal, not clean 429s. Instead every run records `latency_log`. After
merging, compare latency by hour of day against the request rate the project was sending:

```sql
SELECT substr(minute, 12, 2) AS hour, SUM(requests) AS requests,
       ROUND(SUM(total_latency) / SUM(requests), 2) AS avg_latency, ROUND(MAX(max_latency), 1) AS worst,
       SUM(failures) AS failures
FROM latency_log GROUP BY 1 ORDER BY 1;
```

If latency tracks the clock rather than the project's own rate, the slow periods are the court's, and
`workers` can be raised outside them. If it tracks the rate, the ceiling is real.

---

## Performance

| Stage | Throughput | Basis |
|---|---|---|
| Index (search + document listing, all days) | ~45 min, ~20,000 requests | 4 jobs at 5 req/s; search measured at ~19 days/minute per source |
| Fetch | 1 request per ticket; up to ~12,000/hour per worker at 3.3 req/s | measured ~10,800/hour locally |
| OCR | ~1.3 CPU-seconds per ticket → ~11,000/hour on 4 cores | measured on the PC; GitHub runners may be slower |
| All ~2 million tickets, 4 workers | roughly 2–3 days | estimate; OCR-bound |

Compared with the first design (two requests per ticket, four sources at 5 req/s): about the same wall
time for **half the requests** to the court.

Tesseract is limited to one thread per process (`OMP_THREAD_LIMIT=1`). Running many processes that each
start their own thread pool oversubscribed the CPU: 6.3 vs 1.3 CPU-seconds per ticket.

Volume: 279,378 parking citations in 2020; weekdays typically see 1,000–1,600.

---

## Known data gaps and caveats

- **Citations with no ticket document.** Some cases have no parking-citation document at all, so their
  location can't be recovered from the API. On 2024-01-05, 1,247 of 1,258 citations had none (the court
  switched case management systems in early 2024); on test days in March 2020 and June 2025 it was 3% and
  0.2%. The index run's summary shows the count per year, and they stay in the data as `no_doc`:
  ```sql
  SELECT substr(violation_date,1,7) AS month, status, COUNT(*) FROM citations GROUP BY 1, 2;
  ```
  Treat months with a large gap as missing, not as low enforcement.
- **Geocoding misses.** About 2–3% of distinct printed locations returned `No_Match` from the Census
  geocoder even though they look valid (e.g. `1222 HARBOR AVE SW`). A second geocoder could recover them.
- **Handwritten tickets** (~0.5%) have no parsed fields and need manual entry. They may not be a random subset.
- **OCR errors** remain in some fields (e.g. `11.76.0015` for `11.76.015`). `violation` and `smc` carry the
  same section, so they can cross-check each other. `infraction_location` is checked by the geocoder, but a
  misread that is still a valid address passes; hence the QA sample.
- **`infraction_datetime`** is incomplete on legacy tickets. Use `violation_date` (UTC) and convert to
  Seattle time.
- **Snapshot in time.** The index reflects the day it was built: `closed_at_search`, and documents filed
  later, are not updated. Rebuild the index to refresh (finished buckets from the old index then no
  longer count).

---

## Notes for the analysis

- **Partial data is usable.** Finished buckets are a uniform random sample. Weight counts by
  256 / (buckets merged) to estimate totals.
- **Complaint-driven vs patrol-driven.** Public comments such as `CSR COMPLAINT` mark tickets written in
  response to a resident complaint. Those measure resident behaviour as much as enforcement, so analyse them
  separately.
- **Denominators.** Raw counts by tract mostly reflect where parking and parking rules exist. Normalise by
  exposure: SDOT curb-space, paid-parking and restricted parking zone (RPZ) data, blockface counts, or
  on-street parking supply.
- **Demographics.** Join `geoid_tract` to American Community Survey (ACS) 5-year tract tables.
- **Ticket mix.** Compare violation types (`smc`), fines and officers (`officer_id`) across tracts, not just
  totals.
- **Time.** Volumes changed sharply in 2020. Check `citing_agency` and daily volumes (`searched_days`) over
  time before pooling years.
- If the work is published through UW, check with the UW Human Subjects Division (IRB) about whether review
  is needed.

---

## Design history

| Version | Design | Why it was replaced |
|---|---|---|
| 1 (Nov 10) | One machine, year by year: search → fetch → OCR → geocode. | ~40 days for the full range. |
| 2 (Nov 10) | Four static day-shards (`--shard K/4`) on GitHub ×2, a PC and a UW Linux host; each fetched oldest-first with two requests per ticket and carried one growing database between 5-hour GitHub runs. | A stopped machine stalled its quarter; partial results were "the oldest tickets"; 5-hour checkpoints; two requests per ticket; three kinds of runner to maintain. Ran for about two hours (~40,000 tickets) and was discarded. |
| 3 (current) | Index first, then 256 random buckets, one request per ticket, GitHub only, images kept. | |

`--shard K/N` survives only as the way the index build splits days across four jobs.

---

## Ideas not yet built

- **Hourly rate schedule.** Once `latency_log` shows which hours have headroom, vary `workers` or the
  interval by time of day.
- **Second geocoder** for the ~2–3% of addresses the Census geocoder misses (King County or Seattle
  address points).
- **Incremental updates.** For new months, build an index for just that date range and run its buckets; the
  merge already combines any set of bucket databases. Keep bucket counts small for small ranges.
- **Non-GitHub workers.** A PC or server could work buckets too, but it would need a GitHub token to upload
  results and a claim step so two machines don't take the same bucket.
- **Manual entry tool** for handwritten tickets, working from the saved images.

---

## Command reference

Global options (before the subcommand):

| Option | Default | |
|---|---|---|
| `--data-dir` | `data` | Folder holding `parking.db` and `tickets/`. |
| `--interval` | 0.2 | Seconds between requests (5/s). Adapts automatically. |
| `--shard K/N` | `0/1` | `search` / `list-docs`: only days with ordinal % N == K. |
| `--stop-at` | none | Unix time after which search/fetch/geocode start no new work. |

| Command | Options |
|---|---|
| `search --from --to` | Citations by violation day. `--workers 4`, `--sample 0.05`, `--redo` |
| `list-docs --from --to` | Parking-citation documents by filing day. `--workers 4`, `--redo` |
| `missing-days --from --to --docs-to` | Days not yet searched / listed for this shard (0 = complete). |
| `build-index` | Join citations to documents, assign buckets. `--buckets 256`, `--slim` |
| `take-bucket --index FILE --bucket N` | Copy one bucket's citations into `--data-dir` (or resume it). |
| `fetch` | `--workers 16`, `--ocr`, `--ocr-workers N`, `--delete-images [printed\|all]`, `--retry-errors`, `--retry`, `--limit N`, `--with-charges` |
| `ocr` | `--workers N`, `--delete-images [printed\|all]`, `--redo`, `--reparse` (re-parse stored text only) |
| `geocode` | `--batch 1000` |
| `export` | `--out FILE.csv` |
| `status` | Counts by status, days searched, top errors. |
| `remaining` | Tickets not finished (0 = done). With `--from --to`, also this shard's unsearched days. |
| `merge DB ...` | Combine databases into `--data-dir`. |
| `backup --to FILE.db` | Consistent copy while running. |
| `wait-maintenance` | Sleep through an announced portal outage (`--max-minutes` caps the wait). |

---

## Troubleshooting

| Symptom | Cause / fix |
|---|---|
| `buckets` run plans 0 buckets | No `index` artifact yet (run `index.yml`), an announced portal outage, or everything is done. The plan job's log says which. |
| A bucket job failed | Re-dispatch `buckets.yml`; it plans only what is missing and resumes `part-NNN` if there is one. |
| `existing database doesn't match bucket N of this index` | Partial work from an older index; the bucket starts over. Expected after rebuilding the index. |
| Index job fails with `N days could not be completed` | Three passes weren't enough (server trouble). Re-run the workflow. |
| `saw X rows of Y, N repeated across pages` | Paging shifted mid-search (new filings). The day isn't marked done; the next pass retries it. |
| `N documents in one day hits the API's 10,000 cap` | A filing day with ≥10,000 parking-citation documents. Split that day's listing by hour. |
| Rate keeps dropping (`rate cut to ...`) | The server is erroring or slow. Leave it; it recovers on its own within minutes of the server recovering. |
| `Tesseract OCR not found` | Install it, or set `TESSERACT_CMD` to the executable. |
| `sync_results.ps1` says `no index on GitHub yet` | The `index` artifact expired (90 days) or was never built. |
| Images missing for a finished bucket | Its `images-NNN` artifact expired before syncing (30 days). Re-fetch that bucket: delete its `done-NNN` artifact and re-dispatch. |
