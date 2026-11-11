"""
Seattle Municipal Court parking-citation pipeline.

Stages (each is resumable; state lives in a SQLite database):

  search    Page through the public citation search API one day at a time (days in
            parallel) and record parking citations: 9-digit numeric citation numbers
            in the Infraction category, excluding Seattle Animal Control.
  fetch     For each new citation, find its ticket PDF, download it and keep only the
            page-1 ticket image. With --ocr, OCR each image as soon as it lands.
  ocr       OCR ticket images with Tesseract and parse the printed fields (violation,
            SMC section, fine, infraction location, plate state, officer ID, date/time,
            public comments). Handwritten scans are flagged for manual entry.
  geocode   Send unique infraction locations to the U.S. Census batch geocoder and
            attach lat/lon and census tract.
  export    Write a flat CSV for analysis.
  status    Show how many citations are at each stage.
  remaining / backup / merge   Tools for running the date range as day shards
            (--shard K/N) on several machines. See README.md.

Examples (PowerShell):
  python parking_pipeline.py search --from 2025-11-01 --to 2025-11-07
  python parking_pipeline.py fetch --ocr --delete-images
  python parking_pipeline.py geocode
  python parking_pipeline.py export --out parking_citations.csv
"""
import argparse
import csv
import hashlib
import io
import os
import re
import sqlite3
import sys
import threading
import time
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, as_completed, wait
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import requests
from pypdf import PdfReader

API = "https://courtrecords-api.seattle.gov"
COURT = "68f021c4-6a44-4735-9a76-5360b2e8af13"
TZ = ZoneInfo("America/Los_Angeles")
PARKING_CITATION_RE = re.compile(r"^\d{9}$")  # 13-digit = traffic camera, 9A9999999 = officer traffic
# 9-digit infractions from these agencies are not parking tickets (129x Animal Control = "Traffic - MCIS Legacy")
NON_PARKING_AGENCIES = {"Seattle Animal Control"}
CENSUS_BATCH ="https://geocoding.geo.census.gov/geocoder/geographies/addressbatch"

SCHEMA = """
CREATE TABLE IF NOT EXISTS citations (
    citation_number TEXT PRIMARY KEY,
    case_uuid       TEXT NOT NULL,
    case_number     TEXT,
    violation_date  TEXT,
    filed_date      TEXT,
    closed_at_search INTEGER,
    status          TEXT NOT NULL DEFAULT 'new',   -- new | fetched | no_doc | ocr | error
    error           TEXT,
    image_path      TEXT,
    case_classification TEXT,
    statute_number  TEXT,
    statute_description TEXT,
    dispo_type      TEXT,
    dispo_method    TEXT,
    ocr_text        TEXT,
    violation       TEXT,
    smc             TEXT,
    fine            REAL,
    infraction_location TEXT,
    plate_state     TEXT,
    officer_id      TEXT,
    infraction_datetime TEXT,
    public_comments TEXT,
    updated_at      TEXT
);
CREATE TABLE IF NOT EXISTS geocodes (
    location   TEXT PRIMARY KEY,
    match      TEXT,       -- Match | No_Match | Tie
    match_type TEXT,
    matched_address TEXT,
    lon REAL, lat REAL,
    state_fips TEXT, county_fips TEXT, tract TEXT, block TEXT,
    geoid_tract TEXT
);
CREATE TABLE IF NOT EXISTS searched_days (day TEXT PRIMARY KEY, total INTEGER, parking INTEGER);
"""


# ---------------------------------------------------------------- helpers
class Client:
    """Thread-safe requests wrapper with an adaptive, shared rate limit.

    - Request *starts* are spaced at least `interval` apart across ALL threads, so the total
      request rate is fixed however many workers run.
    - Dial back automatically: any 429/5xx, timeout or connection error doubles the interval
      (up to max_interval) and pauses every worker. After `recover_after` clean responses in a
      row the interval eases back 10% toward the floor (`min_interval`) the user asked for.
    """

    SLOW_LATENCY = 10.0  # seconds (smoothed); normal is 0.5-4s. Above this the server is struggling: slow down.

    def __init__(self, min_interval=0.2, retries=4, max_interval=10.0, recover_after=200):
        self.min_interval = min_interval
        self.interval = min_interval
        self.max_interval = max_interval
        self.recover_after = recover_after
        self.retries = retries
        self._lock = threading.Lock()
        self._next_slot = 0.0
        self._paused_until = 0.0
        self._clean_streak = 0
        self.requests = self.failures = 0
        self.latency_total = 0.0
        self._ewma = None
        self._last_slowdown = 0.0
        self._local = threading.local()

    def stats(self):
        avg = self.latency_total / self.requests if self.requests else 0
        return f"{1 / self.interval:.2f} req/s cap, avg latency {avg:.1f}s, {self.failures} failures"

    def _ok(self, latency):
        with self._lock:
            self.requests += 1
            self.latency_total += latency
            self._ewma = latency if self._ewma is None else 0.95 * self._ewma + 0.05 * latency
            if self._ewma > self.SLOW_LATENCY and time.monotonic() - self._last_slowdown > 60:
                # no errors, but responses are slowing: back off before the server starts failing
                self.interval = min(self.max_interval, self.interval * 1.5)
                self._last_slowdown = time.monotonic()
                self._clean_streak = 0
                print(f"  ! server slowing (avg {self._ewma:.1f}s per request); rate cut to "
                      f"{1 / self.interval:.2f} req/s", file=sys.stderr)
                return
            self._clean_streak += 1
            if (self._clean_streak >= self.recover_after and self.interval > self.min_interval
                    and self._ewma < self.SLOW_LATENCY / 2):
                self.interval = max(self.min_interval, self.interval * 0.9)
                self._clean_streak = 0
                print(f"  + {self.recover_after} clean responses; easing up to {1 / self.interval:.2f} req/s",
                      file=sys.stderr)

    def _fail(self, attempt):
        backoff = 5 * 3 ** attempt
        with self._lock:
            self.failures += 1
            self._clean_streak = 0
            self.interval = min(self.max_interval, self.interval * 2)
            self._paused_until = max(self._paused_until, time.monotonic() + backoff)
        return backoff

    def _session(self):
        if not hasattr(self._local, "s"):
            s = requests.Session()
            s.headers["User-Agent"] = "parking-citation-research/1.0"
            self._local.s = s
        return self._local.s

    def _wait_turn(self):
        with self._lock:
            start = max(time.monotonic(), self._next_slot, self._paused_until)
            self._next_slot = start + self.interval
        delay = start - time.monotonic()
        if delay > 0:
            time.sleep(delay)

    def get(self, url, **kw):
        kw.setdefault("timeout", 120)
        for attempt in range(self.retries + 1):
            self._wait_turn()
            t = time.monotonic()
            try:
                r = self._session().get(url, **kw)
                if r.status_code == 429 or r.status_code >= 500:
                    raise requests.HTTPError(f"HTTP {r.status_code}")
                self._ok(time.monotonic() - t)
                return r
            except (requests.RequestException, requests.HTTPError) as e:
                backoff = self._fail(attempt)
                if attempt == self.retries:
                    raise
                print(f"  ! {e}; all workers pausing {backoff}s, rate cut to {1 / self.interval:.2f} req/s",
                      file=sys.stderr)


def connect(data_dir):
    os.makedirs(data_dir, exist_ok=True)
    db = sqlite3.connect(os.path.join(data_dir, "parking.db"), timeout=60)
    db.row_factory = sqlite3.Row
    db.executescript(SCHEMA)
    cols = {r[1] for r in db.execute("PRAGMA table_info(citations)")}
    if "ticket_format" not in cols:  # added after first release; 'printed' | 'handwritten'
        db.execute("ALTER TABLE citations ADD COLUMN ticket_format TEXT")
    if "citing_agency" not in cols:  # e.g. Seattle Department of Transportation, Seattle Police Department
        db.execute("ALTER TABLE citations ADD COLUMN citing_agency TEXT")
    if "doc_link_uuid" not in cols:  # ticket PDF id, so handwritten tickets can be re-downloaded for manual entry
        db.execute("ALTER TABLE citations ADD COLUMN doc_link_uuid TEXT")
    return db


def past_deadline(args):
    """True once --stop-at (unix time) has passed: stop starting new work so state can be saved."""
    return bool(args.stop_at) and time.time() >= args.stop_at


def bounded(pool, fn, items, inflight, args):
    """Like pool.map but keeps at most `inflight` jobs queued and stops submitting at the deadline.
    Yields (item, future) as jobs finish."""
    items, pending = iter(items), {}
    while True:
        while len(pending) < inflight and not past_deadline(args):
            item = next(items, None)
            if item is None:
                break
            pending[pool.submit(fn, item)] = item
        if not pending:
            return
        done, _ = wait(pending, return_when=FIRST_COMPLETED)
        for fut in done:
            yield pending.pop(fut), fut


def now():
    return datetime.now(TZ).isoformat(timespec="seconds")


def in_sample(citation_number, rate):
    """Deterministic sampling so reruns pick the same citations."""
    if rate >= 1:
        return True
    h = int(hashlib.sha1(citation_number.encode()).hexdigest()[:8], 16)
    return h / 0xFFFFFFFF < rate


# ---------------------------------------------------------------- search
def search_day(c, day, sample):
    """Network work for one day (runs in a worker thread). Returns (total, rows); raises on failure."""
    start = datetime(day.year, day.month, day.day, tzinfo=TZ)
    params = {
        "violationDateFrom": start.isoformat(timespec="milliseconds"),
        "violationDateTo": (start + timedelta(days=1) - timedelta(milliseconds=1)).isoformat(timespec="milliseconds"),
        "caseHeader.courtID": COURT,
        "size": 500,
        "sort": "violationDate,asc",
    }
    page, total_pages, total, rows, seen = 0, 1, 0, [], 0
    while page < total_pages:
        j = c.get(f"{API}/courts/cms/citations", params={**params, "page": page}).json()
        total_pages = j["page"]["totalPages"]
        total = j["page"]["totalElements"]
        results = j.get("_embedded", {}).get("results", [])
        seen += len(results)
        for r in results:
            h = r["caseHeader"]
            cn = r["citationNumber"]
            if h.get("caseCategory") != "Infraction" or not PARKING_CITATION_RE.match(cn):
                continue
            if not in_sample(cn, sample):
                continue
            agency = (r.get("citingAgencyActorInstance") or {}).get("displayName")
            if agency in NON_PARKING_AGENCIES:
                continue
            rows.append((cn, h["caseInstanceUUID"], h.get("caseNumber"), r.get("violationDate"),
                         h.get("filedDate"), int(bool(h.get("closedFlag"))),
                         (r.get("citingAgencyActorInstance") or {}).get("displayName")))
        page += 1
    if seen != total:  # pages shifted under us (new filings mid-search); caller retries the day
        raise RuntimeError(f"saw {seen} of {total} results")
    return total, rows


def cmd_search(args):
    """Each day is an independent job: workers page through days in parallel (sharing one rate limit),
    and this thread does all database writes. A day is marked searched only once all its pages are in."""
    db, c = connect(args.data_dir), Client(args.interval)
    # A database belongs to one shard: refuse to mix, or merged results would double-count or miss days.
    db.execute("CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT)")
    shard = f"{args.shard[0]}/{args.shard[1]}"
    have = db.execute("SELECT value FROM meta WHERE key='shard'").fetchone()
    if have and have[0] != shard:
        sys.exit(f"{args.data_dir} holds shard {have[0]}, not {shard}; use a separate --data-dir per shard")
    db.execute("INSERT OR REPLACE INTO meta VALUES ('shard', ?)", (shard,))
    db.commit()
    d, end, days = date.fromisoformat(args.date_from), date.fromisoformat(args.date_to), []
    k, n = args.shard
    while d <= end:
        if d.toordinal() % n == k and (
                args.redo or not db.execute("SELECT 1 FROM searched_days WHERE day=?", (d.isoformat(),)).fetchone()):
            days.append(d)
        d += timedelta(days=1)
    print(f"{len(days)} days to search (shard {k}/{n}) with {args.workers} workers, "
          f"up to {1 / args.interval:.1f} req/s")
    t0, failed = time.monotonic(), []
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        jobs = bounded(pool, lambda d: search_day(c, d, args.sample), days, args.workers * 2, args)
        for i, (d, fut) in enumerate(jobs, 1):
            try:
                total, rows = fut.result()
            except Exception as e:
                failed.append(d)
                print(f"[{i}/{len(days)}] {d}: ERROR {e} (rerun search to retry)", file=sys.stderr)
                continue
            stamp = now()
            db.executemany(
                """INSERT INTO citations (citation_number, case_uuid, case_number, violation_date,
                       filed_date, closed_at_search, citing_agency, updated_at)
                   VALUES (?,?,?,?,?,?,?,?)
                   ON CONFLICT(citation_number) DO UPDATE SET closed_at_search=excluded.closed_at_search,
                       citing_agency=excluded.citing_agency""",
                [(*r, stamp) for r in rows])
            db.execute("INSERT OR REPLACE INTO searched_days VALUES (?,?,?)", (d.isoformat(), total, len(rows)))
            db.commit()
            print(f"[{i}/{len(days)}] {d}: {total} citations, {len(rows)} parking recorded")
    if past_deadline(args):
        print("stopped at --stop-at deadline; rerun to continue")
    print(f"search of {len(days)} days took {time.monotonic() - t0:.0f}s ({c.stats()})"
          + (f"; {len(failed)} days failed: rerun to retry" if failed else ""))


# ---------------------------------------------------------------- fetch
def page1_ticket_png(pdf_bytes):
    """Return the largest image on page 1 as grayscale PNG bytes."""
    reader = PdfReader(io.BytesIO(pdf_bytes))
    imgs = [im for im in reader.pages[0].images if im.image is not None]
    if not imgs:
        return None
    best = max(imgs, key=lambda im: im.image.size[0] * im.image.size[1] * 1000 + len(im.data))
    buf = io.BytesIO()
    best.image.convert("L").save(buf, "PNG", optimize=True)
    return buf.getvalue()


def fetch_one(c, cn, case, img_dir, with_charges):
    """Network work for one citation (runs in a worker thread). Returns a result dict; never raises."""
    try:
        case_url = f"{API}/courts/{COURT}/cms/cases/{case}"
        # One request lists every document on the case; the citation PDF is named after the citation number.
        acc = c.get(f"{API}/courts/cms/docketentrydocumentsaccess", params={
            "page": 0, "size": 50, "sort": "documentName,asc", "caseHeader.courtID": COURT,
            "caseHeader.caseInstanceUUID": case}).json()
        # Current handheld e-tickets (745... series) name the PDF after the citation number; current scanned
        # paper tickets (13x/17x) use "Complaint - Parking Citation". Cases migrated from the legacy MCIS
        # system (~2020 and earlier) use "spd docs" (e-tickets) and "parkers" (scanned paper tickets).
        pdfs = [d for d in acc.get("_embedded", {}).get("results", [])
                if (d.get("documentInfo") or {}).get("contentType") == "application/pdf"]
        docs = ([d for d in pdfs if d.get("documentName") == cn]
                or [d for d in pdfs if "Parking Citation" in (d.get("documentName") or "")]
                or [d for d in pdfs if (d.get("documentName") or "").lower() in ("spd docs", "parkers")])
        if not docs:
            names = sorted({d.get("documentName") or "" for d in pdfs})
            # record what WAS there so unfamiliar document names can be audited later
            return {"status": "no_doc", "error": f"pdfs: {names}"[:500],
                    "msg": f"no citation document (pdfs: {names})"}
        link = docs[0]["documentLinkUUID"]
        pdf = c.get(f"{API}/courts/{COURT}/cms/case/{case}/docketentrydocuments/{link}").content
        if not pdf.startswith(b"%PDF"):
            raise RuntimeError(f"not a PDF: {pdf[:60]!r}")
        png = page1_ticket_png(pdf)
        if png is None:
            raise RuntimeError("no image on page 1")
        path = os.path.join(img_dir, f"{cn}.png")
        with open(path, "wb") as f:
            f.write(png)
        extra = {}
        if with_charges:
            hdr = c.get(case_url).json().get("caseHeader", {})
            ch = c.get(f"{case_url}/charges", params={"page": 0, "size": 20}).json()
            ch = (ch.get("_embedded", {}).get("results") or [{}])[0]
            extra = {"case_classification": hdr.get("caseClassification"),
                     "statute_number": ch.get("statuteNumber"),
                     "statute_description": ch.get("statuteDescription"),
                     "dispo_type": ch.get("dispoType"), "dispo_method": ch.get("dispoMethod")}
        return {"status": "fetched", "image_path": path, "extra": {**extra, "doc_link_uuid": link},
                "msg": f"ok ({len(pdf)//1024} KB pdf -> {len(png)//1024} KB image)"}
    except Exception as e:  # recorded as an error; retry later with --retry
        return {"status": "error", "error": str(e)[:500], "msg": f"ERROR {e}"}


def cmd_fetch(args):
    """Work through citations recorded by `search`. Workers do the network calls in parallel;
    this thread does all database writes (SQLite connections aren't shared across threads).
    With --ocr, each downloaded image is OCR'd right away in a second pool, so fetch and OCR overlap
    and images never pile up (needed on short-lived CI runners)."""
    db, c = connect(args.data_dir), Client(args.interval)
    img_dir = os.path.join(args.data_dir, "tickets")
    os.makedirs(img_dir, exist_ok=True)
    # images from a run that ended before OCR are gone on a fresh runner: refetch those
    lost = [r[0] for r in db.execute("SELECT citation_number, image_path FROM citations WHERE status='fetched'")
            if not (r[1] and os.path.exists(r[1]))]
    if lost:
        db.executemany("UPDATE citations SET status='new', image_path=NULL WHERE citation_number=?",
                       [(cn,) for cn in lost])
        db.commit()
        print(f"{len(lost)} fetched tickets had no image on disk; queued again")
    statuses = ("new", "error", "no_doc") if args.retry else ("new",)
    skip = tuple(NON_PARKING_AGENCIES)  # rows recorded before search filtered these out
    q = (f"SELECT citation_number, case_uuid FROM citations WHERE status IN ({','.join('?' * len(statuses))}) "
         f"AND COALESCE(citing_agency, '') NOT IN ({','.join('?' * len(skip))}) ORDER BY violation_date")
    rows = db.execute(q + (f" LIMIT {int(args.limit)}" if args.limit else ""), statuses + skip).fetchall()
    print(f"{len(rows)} citations to fetch with {args.workers} workers, up to {1 / args.interval:.1f} req/s"
          + (f", OCR inline with {args.ocr_workers} workers" if args.ocr else ""))
    tess = find_tesseract() if args.ocr else None
    ocr_pool = ThreadPoolExecutor(max_workers=args.ocr_workers) if args.ocr else None
    ocr_futures = {}
    t0, counts = time.monotonic(), {}

    def drain_ocr(block):
        done = [f for f in ocr_futures if f.done()] if not block else list(as_completed(ocr_futures))
        for f in done:
            cn, path = ocr_futures.pop(f)
            try:
                store_ocr(db, cn, path, f.result(), args.delete_images)
            except Exception as e:
                print(f"  ! OCR {cn}: {e}", file=sys.stderr)
        db.commit()

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        work = lambda r: fetch_one(c, r["citation_number"], r["case_uuid"], img_dir, args.with_charges)
        for i, (row, fut) in enumerate(bounded(pool, work, rows, args.workers * 2, args), 1):
            cn, res = row["citation_number"], fut.result()
            counts[res["status"]] = counts.get(res["status"], 0) + 1
            extra = res.get("extra") or {}
            sets = "".join(f", {k}=?" for k in extra)
            db.execute(f"UPDATE citations SET status=?, error=?, image_path=COALESCE(?, image_path), updated_at=?{sets} "
                       f"WHERE citation_number=?",
                       (res["status"], res.get("error"), res.get("image_path"), now(), *extra.values(), cn))
            db.commit()
            if ocr_pool and res["status"] == "fetched":
                ocr_futures[ocr_pool.submit(ocr_image, tess, res["image_path"])] = (cn, res["image_path"])
                drain_ocr(block=False)
            rate = i / (time.monotonic() - t0) * 3600
            print(f"[{i}/{len(rows)}] {cn}: {res['msg']}  ({rate:.0f}/hour)",
                  file=sys.stderr if res["status"] == "error" else sys.stdout)
            if i % 100 == 0:
                print(f"  -- {c.stats()}" + (f", {len(ocr_futures)} waiting for OCR" if ocr_pool else ""))
    if ocr_pool:
        drain_ocr(block=True)
        ocr_pool.shutdown()
    if past_deadline(args):
        print("stopped at --stop-at deadline; rerun to continue")
    print(f"done in {time.monotonic() - t0:.0f}s: {counts} ({c.stats()})")


# ---------------------------------------------------------------- ocr
def parse_ticket(text):
    """Parse the fields printed on a Seattle parking Notice of Infraction."""
    # The legacy receipt font has slashed zeros that OCR reads as "@" next to other digits ("@7:19", "11.70.02@")
    text = re.sub(r"(?<=[\d.:/])@|@(?=[\d.:/])", "0", text)
    lines = [l.strip() for l in text.splitlines()]
    nonblank = [l for l in lines if l]

    def after(label_re):
        for idx, l in enumerate(nonblank):
            if re.match(label_re, l, re.I):
                rest = re.sub(label_re, "", l, flags=re.I).strip(" :")
                if rest:
                    return rest
                return nonblank[idx + 1] if idx + 1 < len(nonblank) else None
        return None

    def grab(pattern):
        m = re.search(pattern, text, re.I)
        return m.group(1).strip() if m else None

    out = {
        "violation": after(r"^VIOLATION\s*:"),
        "smc": grab(r"SMC\s*:?\s*([\d.]+\d)"),
        "fine": grab(r"Fine\s*:?\s*\$\s*([\d,]+(?:\.\d\d)?)"),
        # current tickets: "INFRACTION LOCATION:"; legacy (pre-2021) tickets: "LOCATION:"
        "infraction_location": after(r"^(?:INFRACTION\s+)?LOCATION\s*:"),
        # line-anchored and case-sensitive: a blank State: must not fall through to "State of Washington";
        # legacy tickets print "License: ABC1234-WA" instead
        "plate_state": ((re.search(r"^\s*State:[ \t]*([A-Z]{2})[ \t]*$", text, re.M)
                         or re.search(r"License\s*:\s*\S+-([A-Z]{2})\b", text)) or [None, None])[1],
        "officer_id": grab(r"Officer\s+ID\s*:?\s*(\d+)"),
    }
    d = grab(r"(?:Infraction|Citation)\s*Date\s*:?\s*(\d{1,2}/\d{1,2}/\d{4})")
    t = grab(r"Infraction\s*Time\s*:?\s*(\d{1,2}:\d{2})") or grab(r"\bTime\s*:?\s*(\d{1,2}:\d{2})")
    out["infraction_datetime"] = None
    if d:
        try:
            out["infraction_datetime"] = datetime.strptime(f"{d} {t or '00:00'}", "%m/%d/%Y %H:%M").isoformat()
        except ValueError:
            pass
    # OCR sometimes drops the leading "P" of "Public"
    m = re.search(r"u?blic\s+Comments\s*:?(.*?)(?:INFRACTION\s+)?LOCATION\s*:", text, re.I | re.S)
    out["public_comments"] = " ".join(m.group(1).split()) if m else None
    if out["fine"]:
        out["fine"] = float(out["fine"].replace(",", ""))
    if out["infraction_location"]:
        out["infraction_location"] = normalize_location(out["infraction_location"])
    return out


STREET_TYPES = r"(?:AVE|ST|PL|WAY|DR|CT|BLVD|RD|LN|TER|PKWY|CIR)"
DIRECTIONS = r"(?:NE|NW|SE|SW|N|S|E|W)"


def normalize_location(loc):
    """Repair spaces OCR drops in short street names, e.g. '416 11THAVEE' -> '416 11TH AVE E'."""
    loc = " ".join(loc.upper().split())
    loc = re.sub(rf"(\d+(?:ST|ND|RD|TH))({STREET_TYPES})", r"\1 \2", loc)          # 11THAVE -> 11TH AVE
    loc = re.sub(rf"\b({STREET_TYPES})({DIRECTIONS})\b", r"\1 \2", loc)             # AVEE -> AVE E
    return loc


def find_tesseract():
    # Each Tesseract process otherwise starts its own thread pool; with many processes in parallel that
    # oversubscribes the CPU (measured: 6.3 vs 1.3 CPU-seconds per ticket). One thread per process is fastest.
    os.environ.setdefault("OMP_THREAD_LIMIT", "1")
    try:
        import pytesseract
    except ImportError:
        sys.exit("pytesseract is not installed:  python -m pip install pytesseract")
    for p in (os.environ.get("TESSERACT_CMD"), r"C:\Program Files\Tesseract-OCR\tesseract.exe"):
        if p and os.path.exists(p):
            pytesseract.pytesseract.tesseract_cmd = p
            break
    try:
        pytesseract.get_tesseract_version()
    except Exception:
        sys.exit("Tesseract OCR not found. Install it (winget install UB-Mannheim.TesseractOCR) or set TESSERACT_CMD.")
    return pytesseract


def ocr_image(tess, path):
    """OCR one ticket image (runs in a worker thread; Tesseract itself is a separate process)."""
    from PIL import Image
    with Image.open(path) as im:
        if im.width < 1000:  # legacy receipt-style tickets are 400px wide; Tesseract reads them better at 2x
            im = im.resize((im.width * 2, im.height * 2), Image.LANCZOS)
        return tess.image_to_string(im, config="--psm 4")


PARSED_FIELDS = ["violation", "smc", "fine", "infraction_location", "plate_state", "officer_id",
                 "infraction_datetime", "public_comments"]


def store_ocr(db, cn, image_path, text, delete_images=None):
    """Record OCR text and parsed fields. delete_images: None | 'printed' | 'all'.
    The caller commits."""
    # Printed handheld tickets carry an "INFRACTION LOCATION:" (current) or "LOCATION:" (legacy) label.
    # Anything else is a scanned handwritten paper ticket, which Tesseract can't read: its image is kept
    # (unless 'all') and doc_link_uuid lets it be downloaded again for manual entry.
    fmt = "printed" if re.search(r"^\s*(?:INFRACTION\s+)?LOCATION\s*:", text, re.I | re.M) else "handwritten"
    f = parse_ticket(text) if fmt == "printed" else dict.fromkeys(PARSED_FIELDS)
    db.execute(f"""UPDATE citations SET status='ocr', ticket_format=?, ocr_text=?,
                   {', '.join(k + '=?' for k in PARSED_FIELDS)}, updated_at=? WHERE citation_number=?""",
               (fmt, text, *[f[k] for k in PARSED_FIELDS], now(), cn))
    if delete_images == "all" or (delete_images == "printed" and fmt == "printed"):
        if os.path.exists(image_path):
            os.remove(image_path)
        db.execute("UPDATE citations SET image_path=NULL WHERE citation_number=?", (cn,))


def cmd_reparse(db):
    """Re-run the parser over stored OCR text (no Tesseract) after parser fixes."""
    rows = db.execute("SELECT citation_number, ocr_text FROM citations "
                      "WHERE status='ocr' AND ticket_format='printed'").fetchall()
    for r in rows:
        f = parse_ticket(r["ocr_text"])
        db.execute(f"UPDATE citations SET {', '.join(k + '=?' for k in PARSED_FIELDS)} WHERE citation_number=?",
                   (*[f[k] for k in PARSED_FIELDS], r["citation_number"]))
    db.commit()
    print(f"re-parsed {len(rows)} printed tickets")


def cmd_ocr(args):
    db = connect(args.data_dir)
    if args.reparse:
        return cmd_reparse(db)
    tess = find_tesseract()
    statuses = ("fetched", "ocr") if args.redo else ("fetched",)
    rows = db.execute(f"SELECT citation_number, image_path FROM citations WHERE image_path IS NOT NULL "
                      f"AND status IN ({','.join('?' * len(statuses))})", statuses).fetchall()
    print(f"{len(rows)} images to OCR with {args.workers} workers")
    t0 = time.monotonic()
    pool = ThreadPoolExecutor(max_workers=args.workers)
    futures = {pool.submit(ocr_image, tess, r["image_path"]): r for r in rows}
    for i, fut in enumerate(as_completed(futures), 1):
        row = futures[fut]
        try:
            text = fut.result()
        except Exception as e:
            print(f"  ! {row['citation_number']}: {e}", file=sys.stderr)
            continue
        store_ocr(db, row["citation_number"], row["image_path"], text, args.delete_images)
        if i % 200 == 0:
            db.commit()
            print(f"  {i}/{len(rows)} ({i / (time.monotonic() - t0) * 3600:.0f}/hour)")
    pool.shutdown()
    db.commit()
    missing = db.execute("SELECT COUNT(*) FROM citations WHERE status='ocr' AND ticket_format='printed' "
                         "AND infraction_location IS NULL").fetchone()[0]
    hand = db.execute("SELECT COUNT(*) FROM citations WHERE ticket_format='handwritten'").fetchone()[0]
    print(f"done; {missing} printed tickets have no parsed location (check ocr_text); "
          f"{hand} handwritten tickets kept for manual entry")


# ---------------------------------------------------------------- geocode
def cmd_geocode(args):
    db = connect(args.data_dir)
    locs = [r[0] for r in db.execute(
        """SELECT DISTINCT infraction_location FROM citations
           WHERE infraction_location IS NOT NULL
             AND infraction_location NOT IN (SELECT location FROM geocodes)""")]
    print(f"{len(locs)} unique locations to geocode")
    for start in range(0, len(locs), args.batch):
        if past_deadline(args):
            print("stopped at --stop-at deadline; rerun to continue")
            break
        chunk = locs[start:start + args.batch]
        buf = io.StringIO()
        w = csv.writer(buf)
        for i, loc in enumerate(chunk):
            w.writerow([i, loc, "Seattle", "WA", ""])
        for attempt in range(4):
            try:
                r = requests.post(CENSUS_BATCH, files={"addressFile": ("a.csv", buf.getvalue(), "text/csv")},
                                  data={"benchmark": "Public_AR_Current", "vintage": "Current_Current"}, timeout=300)
                r.raise_for_status()
                break
            except requests.RequestException as e:
                if attempt == 3:
                    raise
                print(f"  ! Census geocoder: {e}; retrying in {30 * (attempt + 1)}s", file=sys.stderr)
                time.sleep(30 * (attempt + 1))
        for rec in csv.reader(io.StringIO(r.text)):
            if not rec or not rec[0].isdigit():
                continue
            loc = chunk[int(rec[0])]
            rec += [""] * (12 - len(rec))
            lon = lat = None
            if rec[5]:
                lon, lat = (float(x) for x in rec[5].split(","))
            st, co, tr, bl = rec[8], rec[9], rec[10], rec[11]
            db.execute("INSERT OR REPLACE INTO geocodes VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                       (loc, rec[2], rec[3], rec[4], lon, lat, st, co, tr, bl,
                        (st + co + tr) if tr else None))
        db.commit()
        print(f"  geocoded {min(start + args.batch, len(locs))}/{len(locs)}")
    for row in db.execute("SELECT match, COUNT(*) n FROM geocodes GROUP BY match"):
        print(f"  {row['match']}: {row['n']}")


# ---------------------------------------------------------------- export / status
EXPORT_COLS = ["citation_number", "citing_agency", "ticket_format","case_number", "violation_date", "infraction_datetime", "filed_date",
               "closed_at_search", "violation", "smc", "fine", "infraction_location", "plate_state",
               "officer_id", "public_comments", "case_classification", "statute_number",
               "statute_description", "dispo_type", "dispo_method"]


def cmd_export(args):
    db = connect(args.data_dir)
    cols = ", ".join(f"c.{x}" for x in EXPORT_COLS)
    rows = db.execute(f"""SELECT {cols}, g.match AS geocode_match, g.matched_address, g.lat, g.lon, g.geoid_tract
                          FROM citations c LEFT JOIN geocodes g ON g.location = c.infraction_location
                          WHERE c.status='ocr' ORDER BY c.violation_date""").fetchall()
    with open(args.out, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(rows[0].keys() if rows else EXPORT_COLS)
        w.writerows(rows)
    print(f"wrote {len(rows)} rows to {args.out}")


def cmd_backup(args):
    """Consistent copy of the database (safe while other stages run), e.g. to a OneDrive folder.
    Written to a temp name and then renamed, so a sync client never uploads a half-written file."""
    db = connect(args.data_dir)
    os.makedirs(os.path.dirname(os.path.abspath(args.to)), exist_ok=True)
    tmp = args.to + ".partial"
    out = sqlite3.connect(tmp)
    db.backup(out)
    out.close()
    os.replace(tmp, args.to)
    print(f"backed up to {args.to} ({os.path.getsize(args.to) / 1e6:.0f} MB)")


def cmd_merge(args):
    """Combine shard databases (each covers different days, so rows never collide) into --data-dir."""
    db = connect(args.data_dir)
    for path in args.sources:
        db.execute("ATTACH DATABASE ? AS src", (path,))
        cols = [r[1] for r in db.execute("PRAGMA src.table_info(citations)")]
        sel = ", ".join(cols)
        db.execute(f"INSERT OR REPLACE INTO citations ({sel}) SELECT {sel} FROM src.citations")
        db.execute("INSERT OR REPLACE INTO searched_days SELECT * FROM src.searched_days")
        db.execute("INSERT OR IGNORE INTO geocodes SELECT * FROM src.geocodes")
        n = db.execute("SELECT COUNT(*) FROM src.citations").fetchone()[0]
        db.commit()
        db.execute("DETACH DATABASE src")
        print(f"merged {n} citations from {path}")
    cmd_status(args)


def cmd_remaining(args):
    """Print how much work is left for this shard: unsearched days + citations not yet fetched/OCR'd."""
    db = connect(args.data_dir)
    k, n = args.shard
    d, end = date.fromisoformat(args.date_from), date.fromisoformat(args.date_to)
    searched = {r[0] for r in db.execute("SELECT day FROM searched_days")}
    days = (d + timedelta(i) for i in range((end - d).days + 1))
    days_left = sum(1 for x in days if x.toordinal() % n == k and x.isoformat() not in searched)
    todo = db.execute("SELECT COUNT(*) FROM citations WHERE status IN ('new', 'fetched')").fetchone()[0]
    print(days_left + todo)


def cmd_status(args):
    db = connect(args.data_dir)
    for r in db.execute("SELECT status, COUNT(*) n FROM citations GROUP BY status ORDER BY n DESC"):
        print(f"{r['status']:8} {r['n']}")
    r = db.execute("SELECT MIN(day) a, MAX(day) b, COUNT(*) n, SUM(parking) p FROM searched_days").fetchone()
    print(f"days searched: {r['n']} ({r['a']} .. {r['b']}), parking recorded: {r['p']}")
    for r in db.execute("SELECT error, COUNT(*) n FROM citations WHERE status='error' GROUP BY error ORDER BY n DESC LIMIT 5"):
        print(f"  error x{r['n']}: {r['error']}")


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--data-dir", default="data", help="where parking.db and ticket images live (default: ./data)")
    p.add_argument("--shard", default="0/1", type=lambda s: tuple(int(x) for x in s.split("/")),
                   help="k/N: handle only days whose ordinal %% N == k, so N machines can split the work "
                        "with no overlap (default 0/1 = everything). Use a separate --data-dir per shard.")
    p.add_argument("--interval", type=float, default=0.2,
                   help="minimum seconds between API requests across all workers (default 0.2 = 5 req/s); "
                        "doubles automatically on errors and eases back after clean runs")
    p.add_argument("--stop-at", type=float, help="unix time after which search/fetch start no new work (for CI time limits)")
    sub = p.add_subparsers(dest="cmd", required=True)

    s = sub.add_parser("search")
    s.add_argument("--from", dest="date_from", required=True, help="YYYY-MM-DD (violation date, Seattle time)")
    s.add_argument("--to", dest="date_to", required=True, help="YYYY-MM-DD inclusive")
    s.add_argument("--sample", type=float, default=1.0, help="fraction of parking citations to keep (deterministic)")
    s.add_argument("--redo", action="store_true", help="re-search days already searched")
    s.add_argument("--workers", type=int, default=4, help="days searched in parallel (default 4)")
    s.set_defaults(func=cmd_search)

    s = sub.add_parser("fetch")
    s.add_argument("--limit", type=int)
    s.add_argument("--retry", action="store_true", help="also retry error and no_doc rows")
    s.add_argument("--with-charges", action="store_true", help="also pull classification and disposition (+2 requests each)")
    s.add_argument("--workers", type=int, default=16,
                   help="parallel downloads; total request rate is still capped by --interval (default 16)")
    s.add_argument("--ocr", action="store_true", help="OCR each image as soon as it is downloaded")
    s.add_argument("--ocr-workers", type=int, default=max(1, (os.cpu_count() or 2) - 1),
                   help="parallel Tesseract processes with --ocr (default: CPU count - 1)")
    s.add_argument("--delete-images", nargs="?", const="printed", choices=["printed", "all"],
                   help="with --ocr: delete images after OCR (printed only, or all)")
    s.set_defaults(func=cmd_fetch)

    s = sub.add_parser("ocr")
    s.add_argument("--redo", action="store_true")
    s.add_argument("--delete-images", nargs="?", const="printed", choices=["printed", "all"],
                   help="delete images after OCR: printed tickets only (default when given), or all")
    s.add_argument("--reparse", action="store_true", help="re-parse stored OCR text only (no Tesseract)")
    s.add_argument("--workers", type=int, default=max(1, (os.cpu_count() or 2) - 2),
                   help="parallel Tesseract processes (default: CPU count - 2)")
    s.set_defaults(func=cmd_ocr)

    s = sub.add_parser("geocode")
    s.add_argument("--batch", type=int, default=1000, help="addresses per Census request (max 10000)")
    s.set_defaults(func=cmd_geocode)

    s = sub.add_parser("export")
    s.add_argument("--out", default="parking_citations.csv")
    s.set_defaults(func=cmd_export)

    sub.add_parser("status").set_defaults(func=cmd_status)

    s = sub.add_parser("remaining", help="print the amount of work left for this shard (0 = done)")
    s.add_argument("--from", dest="date_from", required=True)
    s.add_argument("--to", dest="date_to", required=True)
    s.set_defaults(func=cmd_remaining)

    s = sub.add_parser("backup",help="copy the database somewhere safe (e.g. OneDrive) while running")
    s.add_argument("--to", required=True, help="destination .db file")
    s.set_defaults(func=cmd_backup)

    s = sub.add_parser("merge", help="combine shard databases into --data-dir")
    s.add_argument("sources", nargs="+", help="shard parking.db files")
    s.set_defaults(func=cmd_merge)

    args = p.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
