# ============================================================================
#  FYP DEMO - the DATABASE window.  Opened automatically by stage_demo.ps1.
#
#  If the jury asks "show me the database", press ENTER here to run the full
#  labelled tour:  worker pool | jobs | runs + fencing token | stored logs | schema.
#
#  Best answer to give FIRST, though: "the dashboard IS the database" -
#  pool = nodes table, runs table = runs, live logs = run_logs.
# ============================================================================

$host.UI.RawUI.WindowTitle = 'DATABASE  (press Enter for the tour)'
$FYP = Split-Path -Parent $PSScriptRoot     # derived, never hard-coded
$SQL = Join-Path $FYP 'db_tour.sql'

Write-Host ''
Write-Host '  ===================  DATABASE VIEW  ===================' -ForegroundColor Cyan
Write-Host '  If the jury asks to see the database, press ENTER to run the labelled tour:' -ForegroundColor White
Write-Host '    1) worker pool   2) jobs   3) runs + fencing token   4) stored logs   5) schema' -ForegroundColor DarkGray
Write-Host '  Say first: "the dashboard IS the database" (pool=nodes, runs=runs, logs=run_logs).' -ForegroundColor DarkGray
Write-Host '  Run it AFTER you submit the job, so the tables have data.  Ctrl+C to close.' -ForegroundColor DarkGray
Write-Host ''

while ($true) {
  [void](Read-Host '  Press ENTER to run the DB tour')
  Write-Host ''
  Get-Content $SQL | docker exec -i fyp-postgres-1 psql -U fyp -d fyp
  Write-Host ''
  Write-Host '  --- tour complete. Press ENTER to run it again. ---' -ForegroundColor DarkGray
  Write-Host ''
}
