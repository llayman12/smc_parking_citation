# Year-by-year parking citation run: search -> fetch -> OCR -> geocode for each year, then one export.
# Every stage is resumable, so if this is interrupted just run it again: finished days and tickets are skipped.
#
#   .\run_years.ps1                          # 2020 through October 2026
#   .\run_years.ps1 -FirstYear 2023          # pick up from a later year
param(
    [int]$FirstYear = 2020,
    [int]$LastYear = 2026,
    [string]$LastDay = "2026-10-31",
    [string]$DataDir = "C:\SMCData"
)
$ErrorActionPreference = "Continue"
$script = Join-Path $PSScriptRoot "parking_pipeline.py"
$logDir = Join-Path $DataDir "logs"
New-Item -ItemType Directory -Force $logDir | Out-Null

function Step([string]$log, [string[]]$pipelineArgs) {
    $line = "`n=== $(Get-Date -Format s)  parking_pipeline.py $($pipelineArgs -join ' ')"
    Write-Host $line -ForegroundColor Cyan
    Add-Content $log $line
    # -u: unbuffered so progress shows live; stderr merged so warnings land in the log too
    python -u $script --data-dir $DataDir @pipelineArgs 2>&1 | ForEach-Object { "$_" } | Tee-Object -FilePath $log -Append
}

for ($y = $FirstYear; $y -le $LastYear; $y++) {
    $to = if ($y -eq $LastYear) { $LastDay } else { "$y-12-31" }
    $log = Join-Path $logDir "$y.log"
    Write-Host "`n########## $y ($y-01-01 .. $to) ##########" -ForegroundColor Yellow
    Step $log @("search", "--from", "$y-01-01", "--to", $to)
    Step $log @("fetch")
    Step $log @("fetch", "--retry")                 # one more pass over errors / not-yet-filed documents
    Step $log @("ocr", "--delete-images")           # printed-ticket images are deleted once read
    Step $log @("geocode")
    Step $log @("status")
}

Step (Join-Path $logDir "export.log") @("export", "--out", (Join-Path $DataDir "parking_citations.csv"))
