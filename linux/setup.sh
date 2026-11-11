#!/usr/bin/env bash
# One-time setup for a Linux shard runner without root (e.g. a UW shared host with a small home quota).
# Installs Python 3.12 + Tesseract with micromamba under $BASE (default /var/tmp/smc-$USER, local disk,
# outside the home quota). Safe to rerun; rerun it if the machine wipes /var/tmp.
set -euo pipefail
BASE=${BASE:-/var/tmp/smc-$USER}
export MAMBA_ROOT_PREFIX="$BASE/mamba" CONDA_PKGS_DIRS="$BASE/mamba/pkgs"
mkdir -p "$BASE/bin"
cd "$BASE"
if [ ! -x bin/micromamba ]; then
  curl -Ls https://micro.mamba.pm/api/micromamba/linux-64/latest | tar -xj bin/micromamba
fi
if [ ! -x env/bin/tesseract ]; then
  bin/micromamba create -y -q -p "$BASE/env" -c conda-forge \
    python=3.12 tesseract requests pypdf pillow pytesseract
  bin/micromamba clean -a -y -q
fi
"$BASE/env/bin/python" --version
"$BASE/env/bin/tesseract" --version | head -1
du -sh "$BASE"
echo "setup done: $BASE"
