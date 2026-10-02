# ============================================================================
#  RULE 0b - KILL A WORKER PROPERLY.
#
#  Run me:   powershell -ExecutionPolicy Bypass -File scripts\kill_worker.ps1 node-b
#
#  ---------------------------------------------------------------------------
#  WHY THIS SCRIPT EXISTS
#
#  The recovery demonstration is the centrepiece of this project: kill a machine
#  mid-run, and another machine finishes the work with exactly one accepted
#  result. That story depends entirely on the machine actually going away.
#
#  Closing the worker's window, or killing it from Git Bash, does NOT do that.
#  Git Bash kills the MSYS job and leaves the real python.exe running.
#  The agent keeps heart-beating, so the
#  lease never expires, the reaper never marks the run LOST, and nothing is ever
#  re-dispatched. The demo silently does not happen while the room watches
#  nothing - and it reads as the platform failing rather than the kill missing.
#  That is the worst failure available on the day.
#
#  So the kill is Stop-Process, from PowerShell, and this script ASSERTS the
#  process is gone before it reports success. It never says "killed" on the
#  strength of having issued a kill.
# ============================================================================

[CmdletBinding()]
param(
  # node-a / node-b / node-c  (or just a / b / c)
  [Parameter(Mandatory, Position = 0)][string]$Node,
  # Leave the PowerShell window open (kills only the agent process inside it).
  [switch]$KeepWindow
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

function Say ($m) { Write-Host "  $m"        -ForegroundColor Cyan }
function Ok  ($m) { Write-Host "  [OK]   $m" -ForegroundColor Green }
function Die ($m) { Write-Host "`n  [STOP] $m`n" -ForegroundColor Red; exit 1 }

$name = if ($Node -like 'node-*') { $Node } else { "node-$Node" }

# Two separate matches, deliberately narrow.
#
# The agent is the python process, and there are normally TWO of them per
# worker with identical command lines - one the child of the other. Measured on
# 2026-08-15: node-a ran as pids 25236 and 7516, both persistent across repeated
# samples, not a transient subprocess. So killing "the" agent by picking one pid
# leaves the other heart-beating, which is Rule 0b's failure with extra steps.
# Everything matched is killed, and the assertion below is what proves it.
#
# The window is matched on the title this repo's staging script sets, never on
# the agent command line: any shell that merely MENTIONS '-m agent --name node-b'
# matches that - including the shell running this script, which killed itself
# the first time this was tested.
function Get-AgentProcesses {
  @(Get-CimInstance Win32_Process -ErrorAction SilentlyContinue |
    Where-Object { $_.ProcessId -ne $PID -and
                   $_.Name -like 'python*' -and
                   $_.CommandLine -and
                   $_.CommandLine -like '*-m agent*' -and
                   $_.CommandLine -like "*--name $name*" })
}

function Get-WindowProcesses {
  @(Get-CimInstance Win32_Process -ErrorAction SilentlyContinue |
    Where-Object { $_.ProcessId -ne $PID -and
                   $_.CommandLine -and
                   $_.CommandLine -like "*WORKER $name*" })
}

Write-Host "`n============  KILL $($name.ToUpper())  ============`n" -ForegroundColor White

# The heartbeat lives in the python process. The powershell host is only the
# window around it, and killing the window alone is precisely the mistake.
$agents = @(Get-AgentProcesses)
$hosts_ = @(Get-WindowProcesses)

if ($agents.Count -eq 0) {
  Die ("No running agent found for $name. Nothing was killed.`n         " +
       "Either that worker is already down, or it was started under a different " +
       "name. Check the pool at http://localhost:5173 before continuing.")
}

foreach ($p in $agents) { Say "agent   pid $($p.ProcessId)  ($($p.Name))" }
foreach ($p in $hosts_) { Say "window  pid $($p.ProcessId)  ($($p.Name))" }

# The agent first: the heartbeat must stop even if closing the window fails.
foreach ($p in $agents) { Stop-Process -Id $p.ProcessId -Force -ErrorAction SilentlyContinue }
if (-not $KeepWindow) {
  foreach ($p in $hosts_) { Stop-Process -Id $p.ProcessId -Force -ErrorAction SilentlyContinue }
}

# ---- The assertion. This is the whole point of the script. ----------------
$survivors = @()
foreach ($i in 1..10) {
  Start-Sleep -Milliseconds 300
  $survivors = @(Get-AgentProcesses)
  if ($survivors.Count -eq 0) { break }
}

if ($survivors.Count -gt 0) {
  $pids = ($survivors | ForEach-Object { $_.ProcessId }) -join ', '
  Die ("THE AGENT IS STILL RUNNING - pid $pids. It is still heart-beating, so the " +
       "lease will not expire and the run will NOT be re-dispatched.`n         " +
       "Do not continue the demo. Kill it by hand:  Stop-Process -Id $pids -Force")
}

Ok "$name is gone - verified, not assumed. The agent process no longer exists."
# The lease this stack is really running with, read off the control plane's own
# environment (walk 1, row 40: this line used to say "~15 s" whatever the value was;
# the shipped default is 60 s and only the demo script sets 15).
$lease = $null
try { $lease = (& docker compose exec -T control-plane printenv LEASE_TTL_S 2>$null | Out-String).Trim() } catch { $lease = $null }
if ($lease) {
  Say "Its lease expires within $lease s (this stack's LEASE_TTL_S); the reaper then marks the run LOST and requeues it."
} else {
  Say 'Its lease expires within LEASE_TTL_S (60 s by default; scripts\stage_demo.ps1 sets 15); the reaper then marks the run LOST and requeues it.'
}
Say 'Watch the pool go offline, then the run reappear on another machine at attempt 2.'
Write-Host ''
