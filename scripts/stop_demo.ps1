# ============================================================================
#  FYP DEMO - teardown.  Stops the 3 worker windows + the dashboard window.
#  Leaves Docker + the stack running (harmless).  Use after a rehearsal, or
#  after the presentation.
#
#  Run me:   powershell -ExecutionPolicy Bypass -File scripts\stop_demo.ps1
# ============================================================================

$ErrorActionPreference = 'Continue'

function Stop-Matching($label, $pattern) {
  $procs = Get-CimInstance Win32_Process |
           Where-Object { $_.CommandLine -and $_.CommandLine -like $pattern }
  if (-not $procs) { Write-Host "  (no $label found)" -ForegroundColor DarkGray; return }
  foreach ($p in $procs) {
    Stop-Process -Id $p.ProcessId -Force -ErrorAction SilentlyContinue
    Write-Host "  stopped $label  pid=$($p.ProcessId)" -ForegroundColor Yellow
  }
}

Write-Host "`n============  FYP DEMO TEARDOWN  ============`n" -ForegroundColor White

# Worker windows: both the child python.exe and its -NoExit powershell host
# carry '-m agent' in their command line, so this closes the whole window.
Stop-Matching 'worker'    '*-m agent*'
# Dashboard: the node.exe running vite and its npm/powershell host.
Stop-Matching 'dashboard' '*npm run dev*'
Stop-Matching 'dashboard' '*vite*'
# The database GUI (pgAdmin) is a Docker container, not a window - it is left
# running with the rest of the stack (like postgres). Nothing to close here.

Write-Host "`n  Workers + dashboard stopped. Stack + Docker (incl. pgAdmin) left running." -ForegroundColor Green
Write-Host "  Re-stage any time:  powershell -ExecutionPolicy Bypass -File scripts\stage_demo.ps1`n" -ForegroundColor White
