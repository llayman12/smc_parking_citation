# Runs one shard of the parking pipeline on this machine until the shard is finished.
# GitHub Actions runs shards 0 and 1 of 4; run shard 2 on one machine and shard 3 on another:
#
#   .\run_shard.ps1 -Shard 2
#   .\run_shard.ps1 -Shard 3 -DataDir C:\Temp\smc -BackupDir "$env:OneDrive\SMCData"
#
# Works in chunks: each hour it saves a consistent copy of the database to -BackupDir (e.g. a OneDrive
# folder), so only the small working database lives on this machine. Ticket images are deleted as soon as
# they are OCR'd, so disk use stays at roughly the size of the database. Safe to stop (Ctrl-C) and rerun.
param(
    [Parameter(Mandatory)][int]$Shard,
    [int]$Of = 4,
    [string]$DataDir = "C:\SMCData\shard-$Shard-of-$Of",
    [string]$BackupDir = "",
    [string]$From = "2020-01-01",
    [string]$To = "2026-10-31",
    [double]$Interval = 0.2,
    [int]$ChunkMinutes = 60
)
$ErrorActionPreference = "Continue"
$script = Join-Path $PSScriptRoot "parking_pipeline.py"
$common = @("--data-dir", $DataDir, "--shard", "$Shard/$Of", "--interval", "$Interval")
New-Item -ItemType Directory -Force $DataDir | Out-Null
$log = Join-Path $DataDir "run.log"

function Py([string[]]$a) {
    python -u $script @common @a 2>&1 | ForEach-Object { "$_" } | Tee-Object -FilePath $log -Append
}

while ($true) {
    Py @("wait-maintenance")   # sleeps through announced court portal outages
    $stop =[int][double]::Parse((Get-Date -UFormat %s)) + $ChunkMinutes * 60
    Write-Host "`n=== $(Get-Date -Format s)  shard $Shard/$Of chunk until $((Get-Date).AddMinutes($ChunkMinutes).ToString('HH:mm'))" -ForegroundColor Cyan
    Py @("--stop-at", "$stop", "search", "--from", $From, "--to", $To)
    Py @("--stop-at", "$stop", "fetch", "--ocr", "--delete-images", "all")
    Py @("geocode")
    if ($BackupDir) {
        Py @("backup", "--to", (Join-Path $BackupDir "parking-shard-$Shard-of-$Of.db"))
    }
    $left = [int](python $script @common remaining --from $From --to $To)
    Write-Host "shard $Shard/$Of work remaining: $left" -ForegroundColor Yellow
    if ($left -eq 0) { break }
}
Py @("status")
Write-Host "Shard $Shard/$Of finished. Optionally run once more with: python parking_pipeline.py $($common -join ' ') fetch --retry --ocr --delete-images all"
