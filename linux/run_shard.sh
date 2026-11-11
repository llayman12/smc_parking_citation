#!/usr/bin/env bash
# Runs one shard on a Linux machine until it is finished (bash version of run_shard.ps1).
#   tmux new -d -s shard3 'bash ~/smc_parking_citation/linux/run_shard.sh 3'
#
# The working SQLite database lives on local disk ($BASE, default /var/tmp/smc-$USER): outside the home
# quota and off NFS, where SQLite locking is unreliable. Every chunk (default 60 min) a compressed copy goes
# to $BACKUP_DIR (default ~/smc_backup), replacing the previous one. If $BASE is wiped, the next start
# restores from that backup. Runs at low CPU priority so other users of a shared host are not affected.
set -uo pipefail
SHARD=${1:?usage: run_shard.sh SHARD [OF]}
OF=${2:-4}
BASE=${BASE:-/var/tmp/smc-$USER}
DATA="$BASE/shard-$SHARD-of-$OF"
BACKUP_DIR=${BACKUP_DIR:-$HOME/smc_backup}
FROM=${FROM:-2020-01-01}
TO=${TO:-2026-10-31}
INTERVAL=${INTERVAL:-0.2}
CHUNK_MIN=${CHUNK_MIN:-60}
OCR_WORKERS=${OCR_WORKERS:-4}
HERE=$(cd "$(dirname "$0")/.." && pwd)
PY="$BASE/env/bin/python"
export TESSERACT_CMD="$BASE/env/bin/tesseract" TESSDATA_PREFIX="$BASE/env/share/tessdata"
BACKUP="$BACKUP_DIR/parking-shard-$SHARD-of-$OF.db.gz"

[ -x "$PY" ] || { echo "run linux/setup.sh first"; exit 1; }
mkdir -p "$DATA" "$BACKUP_DIR"
if [ ! -f "$DATA/parking.db" ] && [ -f "$BACKUP" ]; then
  echo "restoring $DATA/parking.db from $BACKUP"
  gunzip -c "$BACKUP" > "$DATA/parking.db"
fi

p() { nice -n 19 "$PY" -u "$HERE/parking_pipeline.py" --data-dir "$DATA" --shard "$SHARD/$OF" --interval "$INTERVAL" "$@" 2>&1 | tee -a "$DATA/run.log"; }

while true; do
  stop=$(( $(date +%s) + CHUNK_MIN * 60 ))
  echo "=== $(date -Is) shard $SHARD/$OF chunk until $(date -d @$stop +%H:%M)" | tee -a "$DATA/run.log"
  p --stop-at "$stop" search --from "$FROM" --to "$TO"
  p --stop-at "$stop" fetch --ocr --ocr-workers "$OCR_WORKERS" --delete-images all
  p geocode
  # consistent snapshot -> gzip -> atomic replace, so the home quota only ever holds one backup (+ one in flight)
  p backup --to "$DATA/snapshot.db" && gzip -c "$DATA/snapshot.db" > "$BACKUP.partial" \
    && mv "$BACKUP.partial" "$BACKUP" && rm -f "$DATA/snapshot.db" \
    && echo "backup: $(du -h "$BACKUP" | cut -f1) at $BACKUP" | tee -a "$DATA/run.log"
  left=$("$PY" "$HERE/parking_pipeline.py" --data-dir "$DATA" --shard "$SHARD/$OF" remaining --from "$FROM" --to "$TO")
  echo "shard $SHARD/$OF work remaining: $left" | tee -a "$DATA/run.log"
  [ "$left" = "0" ] && break
done
p status
echo "shard $SHARD/$OF finished"
