# Run by the Windows scheduled task "SMC daily sync" (7:00 AM, after the daily GitHub run has finished):
# pulls new results, merges, geocodes and exports the CSV, keeping a log per day in C:\SMCData\logs.
# To recreate the task:
#   $a = New-ScheduledTaskAction -Execute "pwsh.exe" -Argument "-NoProfile -ExecutionPolicy Bypass -File `"<this file>`""
#   Register-ScheduledTask "SMC daily sync" -Action $a -Trigger (New-ScheduledTaskTrigger -Daily -At 7am) `
#       -Settings (New-ScheduledTaskSettingsSet -StartWhenAvailable -ExecutionTimeLimit (New-TimeSpan -Hours 6))
$logs = "C:\SMCData\logs"
New-Item -ItemType Directory -Force $logs | Out-Null
Start-Transcript -Path (Join-Path $logs ("sync-{0:yyyy-MM-dd}.log" -f (Get-Date))) -Append | Out-Null
try {
    & (Join-Path $PSScriptRoot "sync_results.ps1") -Merge
} finally {
    Stop-Transcript | Out-Null
}
# keep two months of logs
Get-ChildItem $logs -Filter "sync-*.log" | Where-Object LastWriteTime -lt (Get-Date).AddDays(-60) | Remove-Item
