# Pulls finished work from GitHub Actions down to this PC and OneDrive. Safe to run as often as you like:
# anything already downloaded is skipped. GitHub only holds results temporarily (bucket databases 90 days,
# ticket images 30 days), so run this at least every couple of weeks while the pipeline is working.
#
#   .\sync_results.ps1                 # download new bucket databases and image archives
#   .\sync_results.ps1 -Merge          # ...then merge, geocode and export the CSV
#   .\sync_results.ps1 -NoImages       # databases only
#
# What goes where:
#   <OneDrive>\SMCData\images\images-NNN-<id>.zip   ticket images, one zip per bucket job (~450 MB each,
#                                                   ~115 GB in total). Marked online-only once written,
#                                                   so OneDrive uploads them and frees the local disk.
#   <OneDrive>\SMCData\db\done-NNN.db.gz            compressed copy of each finished bucket database
#   C:\SMCData\buckets\done-NNN.db                  working copies used for merging (not in OneDrive: a sync
#                                                   client and a live SQLite file don't mix)
#   C:\SMCData\all\parking.db, C:\SMCData\parking_citations.csv   with -Merge
param(
    [string]$Repo = "llayman12/smc_parking_citation",
    [string]$LocalDir = "C:\SMCData",
    [string]$OneDriveDir = $(if ($env:OneDriveCommercial) { Join-Path $env:OneDriveCommercial "SMCData" } else { Join-Path $env:OneDrive "SMCData" }),
    [switch]$Merge,
    [switch]$NoImages
)
$ErrorActionPreference = "Stop"
$buckets = Join-Path $LocalDir "buckets"
$dbOut = Join-Path $OneDriveDir "db"
$imgOut = Join-Path $OneDriveDir "images"
New-Item -ItemType Directory -Force $buckets, $dbOut, $imgOut | Out-Null

function Get-Artifact([long]$id, [string]$dest) {
    # curl streams the zip straight to disk; written to a temp name first so a half-download is never kept
    $token = gh auth token
    & curl.exe -sS -L --fail -H "Authorization: Bearer $token" -H "Accept: application/vnd.github+json" `
        -o "$dest.partial" "https://api.github.com/repos/$Repo/actions/artifacts/$id/zip"
    if ($LASTEXITCODE -ne 0) { Remove-Item "$dest.partial" -ErrorAction SilentlyContinue; throw "download of artifact $id failed" }
    Move-Item -Force "$dest.partial" $dest
}

$artifacts = gh api --paginate "repos/$Repo/actions/artifacts?per_page=100" `
    -q '.artifacts[] | select(.expired == false) | [.id, .name, .size_in_bytes, .created_at] | @tsv' |
    ForEach-Object { $f = $_ -split "`t"; [pscustomobject]@{ Id = [long]$f[0]; Name = $f[1]; Size = [long]$f[2]; Created = $f[3] } }
# Only results made from the current index count (an earlier index, e.g. a test, cut its buckets differently)
$index = $artifacts | Where-Object Name -eq "index" | Sort-Object Created | Select-Object -Last 1
if (-not $index) { Write-Host "no index on GitHub yet"; return }
$artifacts = @($artifacts | Where-Object { $_.Created -gt $index.Created })

$done = @($artifacts | Where-Object Name -like "done-*")
$images = @($artifacts | Where-Object Name -like "images-*")
Write-Host "$($done.Count) of 256 buckets finished on GitHub; $($images.Count) image archives available"

$newDb = 0
foreach ($a in $done) {
    $db = Join-Path $buckets "$($a.Name).db"
    if (Test-Path $db) { continue }
    $zip = Join-Path $buckets "$($a.Name).zip"
    Get-Artifact $a.Id $zip
    $tmp = Join-Path $buckets "$($a.Name).tmp"
    Expand-Archive -Force $zip $tmp
    Copy-Item -Force (Join-Path $tmp "parking.db.gz") (Join-Path $dbOut "$($a.Name).db.gz")
    python -c "import gzip,shutil,sys; shutil.copyfileobj(gzip.open(sys.argv[1]), open(sys.argv[2],'wb'))" (Join-Path $tmp "parking.db.gz") "$db.partial"
    Move-Item -Force "$db.partial" $db
    Remove-Item -Recurse -Force $zip, $tmp
    $newDb++
    Write-Host "  database $($a.Name)"
}

$newImg = 0
if (-not $NoImages) {
    foreach ($a in $images) {
        $zip = Join-Path $imgOut "$($a.Name)-$($a.Id).zip"
        if (Test-Path $zip) { continue }
        Get-Artifact $a.Id $zip
        attrib.exe +U -P $zip | Out-Null   # online-only: OneDrive uploads it, then frees the local copy
        $newImg++
        Write-Host ("  images   {0} ({1:N0} MB)" -f $a.Name, ($a.Size / 1MB))
    }
}
Write-Host "downloaded $newDb new bucket databases and $newImg new image archives"

if ($Merge) {
    $script = Join-Path $PSScriptRoot "parking_pipeline.py"
    $all = Join-Path $LocalDir "all"
    $dbs = @(Get-ChildItem $buckets -Filter "done-*.db" | ForEach-Object FullName)
    if (-not $dbs) { Write-Host "nothing to merge yet"; return }
    python $script --data-dir $all merge @dbs | Select-Object -Last 8
    python $script --data-dir $all geocode
    python $script --data-dir $all export --out (Join-Path $LocalDir "parking_citations.csv")
    Write-Host ("merged {0} of 256 buckets = a {1:P0} random sample of all tickets" -f $dbs.Count, ($dbs.Count / 256))
}
