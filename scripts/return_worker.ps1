<#
  return_worker.ps1 - bring a STOPPED worker back, reusing its identity.

  This is the "bring a worker back" case, not a first start. The identity file
  is deliberately NOT deleted: deleting it makes the agent register fresh, which
  puts the same machine in the pool TWICE and loses its declared memory (a
  returning node-c would come back reading the host's real RAM instead of
  1024 MB). It refuses to start at all if that file is missing, rather than
  quietly creating a duplicate row in front of the room.

  Rule 0 is not broken by this: the machine was stopped BEFORE the job was
  submitted, so this is a machine joining the pool, not a restart of a worker
  that is mid-run.
#>
param([string]$Node = 'node-c', [string]$RamMb = '1024')

$name  = if ($Node -like 'node-*') { $Node } else { "node-$Node" }
$Root  = Split-Path -Parent $PSScriptRoot          # derived, never hard-coded
$Py    = Join-Path $Root '.venv\Scripts\python.exe'
$Ca    = Join-Path $Root 'certs\ca.pem'
$state = Join-Path $env:TEMP "fyp-$name.json"

Write-Host "`n============  BRING $($name.ToUpper()) BACK  ============`n" -ForegroundColor Cyan

if (-not (Test-Path $state)) {
    Write-Host "  [STOP] No identity file at $state" -ForegroundColor Red
    Write-Host "         Starting $name now would register it as a SECOND machine" -ForegroundColor Red
    Write-Host "         and the pool would show $name twice. Nothing was started." -ForegroundColor Red
    exit 1
}

$live = @(Get-CimInstance Win32_Process -ErrorAction SilentlyContinue |
          Where-Object { $_.Name -like 'python*' -and $_.CommandLine -and
                         $_.CommandLine -like '*-m agent*' -and
                         $_.CommandLine -like "*--name $name*" })
if ($live.Count -gt 0) {
    Write-Host "  [note] $name is ALREADY running (pid $($live[0].ProcessId)). Nothing started." -ForegroundColor Yellow
    exit 0
}

$id = (Get-Content $state -Raw | ConvertFrom-Json).node_id
Write-Host "  Reusing identity $id" -ForegroundColor DarkGray
Write-Host "  (same machine, same row, declared memory unchanged)`n" -ForegroundColor DarkGray

$inner = "`$host.UI.RawUI.WindowTitle='WORKER $name'; Set-Location '$Root'; " +
         "`$env:AGENT_STATE_FILE='$state'; `$env:AGENT_RAM_MB='$RamMb'; " +
         "& '$Py' -m agent --server https://localhost:8000 --ca-cert `"$Ca`" --name $name"
Start-Process powershell -ArgumentList '-NoExit','-Command',$inner | Out-Null

Write-Host "  [OK]   $name starting. It registers within about 3 seconds." -ForegroundColor Green
Write-Host "         Watch it go ONLINE in the pool, then take the waiting run" -ForegroundColor DarkGray
Write-Host "         at ATTEMPT 2 and finish it." -ForegroundColor DarkGray
Write-Host ""
