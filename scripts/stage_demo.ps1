# ============================================================================
#  FYP DEMO - one-command staging.
#
#  Open Docker Desktop, wait for "Engine running", then run me. I do the rest.
#
#  Run me:   powershell -ExecutionPolicy Bypass -File scripts\stage_demo.ps1
#  Just the schema check, nothing touched:
#            powershell -ExecutionPolicy Bypass -File scripts\stage_demo.ps1 -CheckSchemaOnly
#
#  ---------------------------------------------------------------------------
#  WHY THIS SCRIPT LOOKS THE WAY IT DOES (read before changing the order)
#
#  On 2026-08-15 this script could not run at all. It hard-coded a project
#  folder that no longer existed, and with $ErrorActionPreference at 'Continue'
#  it walked straight past the dead path, brought the stack up in whatever
#  directory it happened to be launched from, WIPED THE DATABASE, and only then
#  failed on the venv. It destroyed first and validated second.
#
#  That is the defect this file is shaped around, and the fix is not the path.
#  The rules now are:
#
#    1. VALIDATE, THEN DESTROY, THEN BUILD. Everything the run will need is
#       checked before anything irreversible happens. Enforced by the shell
#       ($ErrorActionPreference = 'Stop', strict mode), not by care.
#    2. NO MACHINE'S LAYOUT IS WRITTEN INTO THIS FILE. The repo root is derived
#       from the script's own location, and the postgres container is found
#       through this repo's own compose project - never by a guessed name.
#    3. THE WIPE IS ENUMERATED. Every table is named. TRUNCATE ... CASCADE stays
#       behind the list as the belt, and a check reads the live schema and fails
#       when the list and the database disagree. A table list is a number that
#       will move again, so it gets a check that reads it rather than a comment
#       asking someone to remember.
#    4. IDEMPOTENT. Runs from any state - mid-demo, half-broken, fully clean -
#       and converges on exactly 3 workers and 1 dashboard.
#
#  What the safety check does and does not promise: it can only reach the
#  postgres container that THIS repository's compose file owns, the database
#  must be named fyp, and its table set must match the schema below exactly. It
#  is not a guarantee against a compose file that has been pointed somewhere it
#  should not be - it is a guarantee that a stray terminal in another project
#  cannot get here.
# ============================================================================

[CmdletBinding()]
param(
  # Run the wipe-list-vs-schema check and exit. Touches nothing.
  [switch]$CheckSchemaOnly,
  # Do not open the pgAdmin browser tab (used when measuring the staging time).
  [switch]$NoBrowser
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

# --- The wipe list. Enumerated on purpose - see rule 3 in the header. --------
#
# WIPE: reset to empty before every demo, so the pool shows exactly the three
# fresh workers and no run from a previous rehearsal.
#   job_keys and key_tickets used to be reached only by CASCADE (they carry
#   foreign keys to jobs and runs). They are named here as well, so the wipe is
#   complete by enumeration and not by inference. CASCADE stays as the belt.
$WIPE_TABLES = @(
  'nodes', 'jobs', 'runs', 'run_logs', 'artifacts',
  'run_samples', 'node_events', 'job_keys', 'key_tickets',
  'run_log_archives'
)

# KEEP: deliberately survives the wipe.
#   users            - holds the bootstrap admin the dashboard logs in as. Wipe
#                      it and the control plane has no account until it restarts.
#   tiers            - the storage tiers (2026-09-04). Every user row points at one
#                      by foreign key, so wiping it would either fail on that key or
#                      leave every account naming a tier that does not exist. They
#                      are configuration, not demonstration data.
#   alembic_version  - the migration pointer. Wipe it and the control plane
#                      believes the database has never been migrated.
$KEEP_TABLES = @('users', 'tiers', 'alembic_version')

# --- Paths, all derived from where this file actually is --------------------
$Root        = Split-Path -Parent $PSScriptRoot
$ComposeFile = Join-Path $Root 'docker-compose.yml'
$VenvPython  = Join-Path $Root '.venv\Scripts\python.exe'
$WebDir      = Join-Path $Root 'web'
$DummyDir    = Join-Path $Root 'workloads\dummy'
# Encrypted transport (2026-08-22). The scheme follows what is actually on disk:
# generate the demo LAN's certificates and every address below becomes https, the
# control plane serves TLS (control-plane/Dockerfile) and the dashboard does too
# (web/vite.config.js). No certificates means plain http, exactly as before.
# The DEMONSTRATION lease (2026-08-29). The shipped default is 60s: a machine that
# drops for under a minute should carry on rather than be fenced out. Sixty seconds is
# right for a real job and wrong for a five-minute slot, because the jury would watch a
# frozen screen for a minute during the recovery beat. So the demo runs at 15s and says
# so out loud.
#
# It is set HERE rather than left to the shell, and that is the point. The demo's
# fallback for seven of eight failure rows is a live re-stage of this very script
#. A presenter who re-stages mid-demo from a
# fresh window must get 15s again without remembering anything. An environment
# variable would not survive that; a constant here does.
#
# This adds no flag and no second path: the script still does
# one thing, one way, and its measured re-stage time is untouched. The READY banner
# prints the effective value so a presenter can SEE which lease they are about to
# demonstrate on, rather than discovering it during the kill.
$DemoLeaseTtlS = 15

$CertDir     = Join-Path $Root 'certs'
$CaFile      = Join-Path $CertDir 'ca.pem'
$ServerCert  = Join-Path $CertDir 'server.pem'
$ServerKey   = Join-Path $CertDir 'server.key'
$script:TlsOn = (Test-Path -LiteralPath $ServerCert) -and (Test-Path -LiteralPath $ServerKey)
$TlsOn       = $script:TlsOn
$Scheme      = if ($TlsOn) { 'https' } else { 'http' }
$Api         = "${Scheme}://localhost:8000"
$DashUrl     = "${Scheme}://localhost:5173"
$DbUser      = 'fyp'
$DbName      = 'fyp'
$ComposeBase = @('compose', '-f', $ComposeFile, '--profile', 'tools')

$PgContainer = $null   # discovered from compose, never guessed

function Say ($m) { Write-Host "  $m"          -ForegroundColor Cyan }
function Ok  ($m) { Write-Host "  [OK]   $m"   -ForegroundColor Green }
function Warn($m) { Write-Host "  [note] $m"   -ForegroundColor Yellow }
function Die ($m) {
  Write-Host "`n  [STOP] $m" -ForegroundColor Red
  Write-Host "  Nothing was changed by this run unless a later step said so.`n" -ForegroundColor Red
  exit 1
}

# Native commands do not raise on failure in PowerShell 5.1, and their stderr
# becomes a terminating error under 'Stop'. So every external call goes through
# here: 'Continue' just for the call, then an explicit exit-code check.
function Invoke-Native {
  param([Parameter(Mandatory)][string]$Exe, [string[]]$Arguments = @())
  $prev = $ErrorActionPreference
  $ErrorActionPreference = 'Continue'
  try {
    $out  = & $Exe @Arguments 2>&1
    $code = $LASTEXITCODE
  } finally {
    $ErrorActionPreference = $prev
  }
  [pscustomobject]@{ ExitCode = $code; Output = ($out | Out-String) }
}

function Invoke-Compose([string[]]$Arguments) {
  Invoke-Native 'docker' ($ComposeBase + $Arguments)
}

function Invoke-Api {
  <#
    One GET/POST against the control plane, returning parsed JSON, or $null if the
    call did not succeed.

    Over plain http this is Invoke-RestMethod, exactly as before. Over https it is
    the venv Python instead, because PowerShell 5.1 can only validate against the
    Windows trust store and we are not prepared to stop validating. The Python path
    verifies against the very same certs\ca.pem the workers use, so the script and
    the workers agree about who they are talking to.
  #>
  param(
    [Parameter(Mandatory)][string]$Url,
    [string]$Method = 'GET',
    [string]$Body,
    [string]$Token,
    [int]$TimeoutSec = 5
  )
  if (-not $script:TlsOn) {
    try {
      $headers = @{}
      if ($Token) { $headers['Authorization'] = "Bearer $Token" }
      if ($Method -eq 'POST') {
        return Invoke-RestMethod $Url -Method Post -ContentType 'application/json' `
                 -Body $Body -Headers $headers -TimeoutSec $TimeoutSec
      }
      return Invoke-RestMethod $Url -Headers $headers -TimeoutSec $TimeoutSec
    } catch { return $null }
  }

  # Everything reaches Python through the ENVIRONMENT, not the command line.
  # PowerShell rewrites quoting when it hands arguments to a native executable, so
  # a JSON body passed as an argument arrives with its quotes stripped and the
  # request fails with a puzzling 422. The environment is passed through verbatim.
  $py = @'
import json, os, ssl, sys, urllib.error, urllib.request
url = os.environ["FYP_API_URL"]
method = os.environ.get("FYP_API_METHOD", "GET")
ca = os.environ["FYP_API_CA"]
timeout = float(os.environ.get("FYP_API_TIMEOUT", "5"))
body = os.environ.get("FYP_API_BODY") or None
token = os.environ.get("FYP_API_TOKEN") or None
ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
ctx.verify_mode = ssl.CERT_REQUIRED
ctx.check_hostname = True
ctx.load_verify_locations(cafile=ca)
req = urllib.request.Request(url, data=body.encode() if body else None, method=method)
if body:
    req.add_header("Content-Type", "application/json")
if token:
    req.add_header("Authorization", "Bearer " + token)
try:
    with urllib.request.urlopen(req, timeout=timeout, context=ctx) as r:
        raw = r.read().decode("utf-8")
    sys.stdout.write(raw if raw else "{}")
except Exception:
    sys.exit(1)
'@
  $tmp = Join-Path $env:TEMP 'fyp-api-call.py'
  Set-Content -LiteralPath $tmp -Value $py -Encoding utf8
  $env:FYP_API_URL     = $Url
  $env:FYP_API_METHOD  = $Method
  $env:FYP_API_CA      = $CaFile
  $env:FYP_API_TIMEOUT = "$TimeoutSec"
  $env:FYP_API_BODY    = $(if ($Body)  { $Body }  else { '' })
  $env:FYP_API_TOKEN   = $(if ($Token) { $Token } else { '' })
  try {
    $res = Invoke-Native $VenvPython @($tmp)
  } finally {
    # The token is a credential: do not leave it in this shell's environment.
    Remove-Item Env:FYP_API_BODY, Env:FYP_API_TOKEN -ErrorAction SilentlyContinue
  }
  if ($res.ExitCode -ne 0 -or -not $res.Output) { return $null }
  try { return ($res.Output | ConvertFrom-Json) } catch { return $null }
}

function Test-UrlReachable {
  <#
    Is this URL answering? Over https it verifies against certs\ca.pem via the venv
    Python, for the same reason Invoke-Api does: PowerShell 5.1 can only validate
    against the Windows trust store, and this check must not report a broken
    dashboard when the only thing missing is a trust step the operator has not done
    yet. A false alarm at T-45 costs more than the check is worth.
  #>
  param([Parameter(Mandatory)][string]$Url, [int]$TimeoutSec = 3)
  if (-not $script:TlsOn) {
    try { Invoke-WebRequest $Url -TimeoutSec $TimeoutSec -UseBasicParsing | Out-Null; return $true }
    catch { return $false }
  }
  $py = @'
import os, ssl, sys, urllib.request
ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
ctx.verify_mode = ssl.CERT_REQUIRED
ctx.check_hostname = True
ctx.load_verify_locations(cafile=os.environ["FYP_URL_CA"])
try:
    urllib.request.urlopen(os.environ["FYP_URL"],
                           timeout=float(os.environ["FYP_URL_TIMEOUT"]), context=ctx).read(1)
except Exception:
    sys.exit(1)
'@
  $tmp = Join-Path $env:TEMP 'fyp-url-check.py'
  Set-Content -LiteralPath $tmp -Value $py -Encoding utf8
  $env:FYP_URL = $Url; $env:FYP_URL_CA = $CaFile; $env:FYP_URL_TIMEOUT = "$TimeoutSec"
  $res = Invoke-Native $VenvPython @($tmp)
  return ($res.ExitCode -eq 0)
}

function Invoke-Psql([string]$Sql) {
  if (-not $PgContainer) { Die 'internal: the postgres container was never resolved.' }
  Invoke-Native 'docker' @(
    'exec', '-i', $PgContainer, 'psql', '-U', $DbUser, '-d', $DbName,
    '-t', '-A', '-v', 'ON_ERROR_STOP=1', '-c', $Sql
  )
}

# Command output goes into error messages, and docker's is hundreds of lines of
# build log. Under pressure the operator needs the last few lines, not all of it.
function Get-Tail([string]$Text, [int]$Lines = 8) {
  $all = @($Text -split "`r?`n" | Where-Object { $_.Trim() })
  if ($all.Count -le $Lines) { return ($all -join [Environment]::NewLine + '         ') }
  ('... (' + ($all.Count - $Lines) + ' earlier lines omitted)' + [Environment]::NewLine + '         ' +
   (($all | Select-Object -Last $Lines) -join ([Environment]::NewLine + '         ')))
}

function Get-Lines([string]$Text) {
  # EVERY call site wraps this in @(). It has to: PowerShell unrolls a
  # one-element array to a bare string on return, and under strict mode .Count
  # then throws while [0] silently hands back a single CHARACTER. Do not also
  # add a leading comma here - the two together nest the array one level deep,
  # and the schema check then reports "System.Object[]" as a missing table.
  @($Text -split "`r?`n" | ForEach-Object { $_.Trim() } | Where-Object { $_ })
}

# Which ports THIS repo's compose project is already publishing. This is the
# reliable way to answer "is that listener ours?": the process name is not, as
# Docker Desktop publishes through com.docker.backend on one machine and
# wslrelay on another, and that varies by version and backend.
$OurPorts = New-Object 'System.Collections.Generic.HashSet[int]'

function Update-OurPublishedPorts {
  $r = Invoke-Compose @('ps', '--format', 'json')
  if ($r.ExitCode -ne 0) { return }
  # Compose emits one JSON object per line on current versions and a single JSON
  # array on older ones. Accept either rather than depending on the version.
  $objects = @()
  try {
    $whole = $r.Output | ConvertFrom-Json
    $objects = @($whole)
  } catch {
    foreach ($line in @(Get-Lines $r.Output)) {
      try { $objects += ($line | ConvertFrom-Json) } catch { }
    }
  }
  foreach ($o in $objects) {
    if (-not $o) { continue }
    if ($o.PSObject.Properties.Name -notcontains 'Publishers') { continue }
    foreach ($p in @($o.Publishers)) {
      if ($p -and $p.PSObject.Properties.Name -contains 'PublishedPort' -and $p.PublishedPort) {
        [void]$OurPorts.Add([int]$p.PublishedPort)
      }
    }
  }
}

# Free, or held by this stack. A foreign listener is the failure we want named
# before the stack comes up, not after docker fails to bind with a wall of text.
#
# Known limit, stated rather than hidden: the fallback owner list below lets any
# Docker port-relay process through, so a container from an UNRELATED compose
# project sitting on one of our ports is not caught here. Compose then fails to
# bind and this script reports that failure with its own message - later than we
# would like, but never silently.
function Assert-PortUsable([int]$Port, [string]$What, [string[]]$AllowedOwners) {
  $conn = @(Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction SilentlyContinue)
  if ($conn.Count -eq 0) { return }
  if ($OurPorts.Contains($Port)) { return }   # already published by our own stack
  foreach ($c in $conn) {
    $proc = Get-Process -Id $c.OwningProcess -ErrorAction SilentlyContinue
    $name = if ($proc) { $proc.ProcessName } else { "pid $($c.OwningProcess)" }
    if ($AllowedOwners -notcontains $name) {
      Die ("Port $Port ($What) is already held by '$name' (pid $($c.OwningProcess)), " +
           "which is not part of this stack. Close it and re-run me.")
    }
  }
}

# ---------------------------------------------------------------------------
#  THE SCHEMA CHECK
#  Compares the enumerated lists above against the tables actually present.
#  It fails loudly on disagreement, and it fails loudly when it cannot read its
#  own input - a check that quietly passes because it found nothing is worse
#  than no check at all.
# ---------------------------------------------------------------------------
function Test-WipeListMatchesSchema {
  $r = Invoke-Psql "SELECT tablename FROM pg_tables WHERE schemaname = 'public' ORDER BY tablename;"
  if ($r.ExitCode -ne 0) {
    Die ("The schema check could not read the table list from the database, so it " +
         "cannot say whether the wipe is complete. It refuses to guess.`n         " +
         "psql said: " + (Get-Tail $r.Output 4))
  }

  $actual = @(Get-Lines $r.Output)
  if ($actual.Count -eq 0) {
    Die ("The schema check read the table list and found NO tables in schema " +
         "'public'. That is not a clean database, it is an unmigrated or wrong " +
         "one. Refusing to continue.")
  }

  $known   = @($WIPE_TABLES + $KEEP_TABLES)
  $unknown = @($actual      | Where-Object { $known  -notcontains $_ })
  $missing = @($WIPE_TABLES | Where-Object { $actual -notcontains $_ })
  $stale   = @($KEEP_TABLES | Where-Object { $actual -notcontains $_ })

  if ($unknown.Count -gt 0) {
    Die ("SCHEMA DRIFT. These tables exist in the database and are in neither the " +
         "wipe list nor the keep list:" + [Environment]::NewLine + "           " +
         ($unknown -join ', ') + [Environment]::NewLine +
         "         A table nobody listed is a table the wipe may miss - that is " +
         "the 2026-08-12 chaos-test defect waiting to happen." + [Environment]::NewLine +
         "         Add each one to the WIPE or KEEP list at the top of " +
         "scripts\stage_demo.ps1, then re-run me.")
  }
  if ($missing.Count -gt 0) {
    Die ("STALE WIPE LIST. These tables are listed for wiping but are not in the " +
         "database:" + [Environment]::NewLine + "           " + ($missing -join ', ') +
         [Environment]::NewLine +
         "         Either the migration that removed them also needs removing " +
         "from the WIPE list, or this is the wrong database.")
  }
  if ($stale.Count -gt 0) {
    Die ("STALE KEEP LIST. These tables are listed as preserved but are not in the " +
         "database:" + [Environment]::NewLine + "           " + ($stale -join ', '))
  }

  Ok ("Schema check: $($actual.Count) tables, all accounted for " +
      "($($WIPE_TABLES.Count) wiped, $($KEEP_TABLES.Count) preserved).")
}

# ===========================================================================
#  PHASE 1 - PRE-FLIGHT.  Reads only. Nothing here changes anything.
# ===========================================================================
$sw = [System.Diagnostics.Stopwatch]::StartNew()
Write-Host "`n============  FYP DEMO STAGING  ============`n" -ForegroundColor White
Say "Repo root (from this script's own location): $Root"

Say 'Pre-flight: checking everything this run will need...'

# Identity: is this actually our repository, or a folder that happens to exist?
foreach ($needed in @($ComposeFile,
                      (Join-Path $Root 'control-plane\app\models.py'),
                      (Join-Path $Root 'scripts\stage_demo.ps1'))) {
  if (-not (Test-Path -LiteralPath $needed)) {
    Die "This does not look like the FYP repository - missing: $needed"
  }
}

if (-not (Test-Path -LiteralPath $VenvPython)) {
  Die ("No Python virtual environment at $VenvPython." + [Environment]::NewLine +
       "         Create it:  python -m venv .venv ; .venv\Scripts\pip install -r agent\requirements.txt")
}
$pyv = Invoke-Native $VenvPython @('--version')
if ($pyv.ExitCode -ne 0) { Die "The venv python at $VenvPython will not run." }

$agentCheck = Invoke-Native $VenvPython @('-c', 'import docker, agent')
if ($agentCheck.ExitCode -ne 0) {
  Die ("The venv is missing the agent's dependencies (import docker, agent failed)." +
       [Environment]::NewLine +
       "         Fix:  .venv\Scripts\pip install -r agent\requirements.txt" +
       [Environment]::NewLine + "         " + $agentCheck.Output.Trim())
}

if (-not (Test-Path -LiteralPath (Join-Path $WebDir 'package.json'))) { Die "No web app at $WebDir." }
if (-not (Test-Path -LiteralPath (Join-Path $WebDir 'node_modules'))) {
  Die "The dashboard's dependencies are not installed. Fix:  cd web ; npm install"
}
if (-not (Get-Command npm -ErrorAction SilentlyContinue)) { Die 'npm is not on PATH - the dashboard cannot start.' }
if (-not (Test-Path -LiteralPath (Join-Path $DummyDir 'Dockerfile'))) { Die "No workload image source at $DummyDir." }

$dockerInfo = Invoke-Native 'docker' @('info', '--format', '{{.ServerVersion}}')
if ($dockerInfo.ExitCode -ne 0) {
  Die 'Docker is not running. Open Docker Desktop, wait for "Engine running", then re-run me.'
}

$cfg = Invoke-Compose @('config', '-q')
if ($cfg.ExitCode -ne 0) { Die ("docker-compose.yml is not valid:" + [Environment]::NewLine + "         " + (Get-Tail $cfg.Output)) }

# Ports. Five are published by our containers, so ask compose which it already
# holds; the process names are only the fallback. 5173 is the Vite dev server,
# which runs on the host as node, so there is nothing for compose to say.
Update-OurPublishedPorts
$dockerRelays = @('com.docker.backend', 'wslrelay', 'vpnkit', 'Docker Desktop Backend', 'dockerd')
# --- encrypted transport ------------------------------------------------------
# Read-only, and before the wipe: a certificate problem must stop the run while the
# database is still intact, not after it has been emptied.
if ($TlsOn) {
  if (-not (Test-Path -LiteralPath $CaFile)) {
    Die ("Found $ServerCert but no $CaFile." + [Environment]::NewLine +
         "The workers verify the control plane against ca.pem, so it must be there." +
         [Environment]::NewLine + "Regenerate both: python scripts\make_certs.py --force")
  }
  $caText = Get-Content -LiteralPath $CaFile -Raw
  if ($caText -notmatch 'BEGIN CERTIFICATE') {
    Die "$CaFile does not look like a PEM certificate. Regenerate: python scripts\make_certs.py --force"
  }
  Ok "TLS on. Workers and browser will verify against certs\ca.pem."
  Warn 'If the browser shows a certificate warning, the CA is not trusted on this machine yet - see docs/DEMO_RUNBOOK.md pre-flight.'
} else {
  Warn 'TLS OFF - traffic is plain http. Generate certificates with: python scripts\make_certs.py'
}

Assert-PortUsable 8000 'control plane'  $dockerRelays
Assert-PortUsable 5432 'postgres'       $dockerRelays
Assert-PortUsable 9000 'MinIO API'      $dockerRelays
Assert-PortUsable 9001 'MinIO console'  $dockerRelays
Assert-PortUsable 5050 'pgAdmin'        $dockerRelays
Assert-PortUsable 5173 'dashboard'      @('node')

Ok ("Pre-flight passed (Docker engine " + $dockerInfo.Output.Trim() + ", python " + $pyv.Output.Trim() + ").")

# ===========================================================================
#  PHASE 2 - BRING UP.  Constructive: starts containers, changes no data.
# ===========================================================================
Say 'Bringing up the stack (postgres + minio + control-plane + pgAdmin)...'
# --build so the control-plane image carries the W6 deps (minio/PyJWT/bcrypt/
# python-multipart). The layer cache makes this a no-op once built.
# docker-compose.yml reads `${LEASE_TTL_S:-60}`, so setting the variable for this
# process is all it takes; without it the stack would come up on the shipped 60s.
$env:LEASE_TTL_S = "$DemoLeaseTtlS"
$up = Invoke-Compose @('up', '-d', '--build')
if ($up.ExitCode -ne 0) { Die ("docker compose up failed:" + [Environment]::NewLine + "         " + (Get-Tail $up.Output)) }
Ok 'Stack containers up (incl. the pgAdmin database GUI).'

$agentTls = if ($TlsOn) { " --ca-cert `"$CaFile`"" } else { '' }
Say 'Waiting for the control plane (:8000/health)...'
$healthy = $false
foreach ($i in 1..45) {
  $h = Invoke-Api -Url "$Api/health" -TimeoutSec 3
  if ($h -and $h.status -eq 'ok') { $healthy = $true; break }
  Start-Sleep -Seconds 1
}
if (-not $healthy) {
  $hint = 'Check: docker compose logs control-plane'
  if ($TlsOn) {
    $hint += [Environment]::NewLine +
             '  TLS is on. If the log says "TLS OFF", the container started before the' +
             [Environment]::NewLine +
             '  certificates existed - restart it: docker compose restart control-plane'
  }
  Die ("Control plane did not become healthy." + [Environment]::NewLine + '  ' + $hint)
}
Ok 'Control plane healthy.'

# ===========================================================================
#  PHASE 3 - THE GATE BEFORE THE WIPE.  The last chance to refuse.
# ===========================================================================
$pgLookup = Invoke-Compose @('ps', '-q', 'postgres')
if ($pgLookup.ExitCode -ne 0) { Die 'Could not ask compose for the postgres container.' }
$pgIds = @(Get-Lines $pgLookup.Output)
if ($pgIds.Count -ne 1) {
  Die ("Expected exactly one postgres container in this compose project, found " +
       "$($pgIds.Count). Refusing to touch a database I cannot identify.")
}
$PgContainer = $pgIds[0]

$who = Invoke-Psql 'SELECT current_database() || pg_catalog.chr(124) || current_user;'
if ($who.ExitCode -ne 0) { Die ("Postgres is up but not answering queries:" + [Environment]::NewLine + "         " + (Get-Tail $who.Output 4)) }
$whoLine = @(Get-Lines $who.Output)[0]
if ($whoLine -ne "$DbName|$DbUser") {
  Die "Refusing to wipe: expected database '$DbName' as user '$DbUser', got '$whoLine'."
}
Ok "Target database confirmed: $DbName as $DbUser, in this repo's own compose project."

Test-WipeListMatchesSchema

if ($CheckSchemaOnly) {
  Write-Host "`n  Schema check only - nothing was changed.`n" -ForegroundColor Green
  exit 0
}

# ===========================================================================
#  PHASE 4 - WIPE.  The only destructive step, and everything above gates it.
# ===========================================================================
Say 'Resetting demo data (so the pool shows exactly the 3 fresh workers)...'
$wipeSql = 'TRUNCATE ' + ($WIPE_TABLES -join ', ') + ' RESTART IDENTITY CASCADE;'
$wipe = Invoke-Psql $wipeSql
if ($wipe.ExitCode -ne 0) { Die ("The wipe failed:" + [Environment]::NewLine + "         " + (Get-Tail $wipe.Output 4)) }

# Any workers or dashboard left over from a previous staging are stopped, so a
# re-stage converges on exactly 3 workers and 1 dashboard instead of stacking up
# a second set of windows on top of the first.
$stale = @(Get-CimInstance Win32_Process -ErrorAction SilentlyContinue |
           Where-Object { $_.CommandLine -and
                          ($_.CommandLine -like '*-m agent*' -or
                           $_.CommandLine -like '*npm run dev*' -or
                           $_.CommandLine -like '*vite*') })
foreach ($p in $stale) { Stop-Process -Id $p.ProcessId -Force -ErrorAction SilentlyContinue }
if ($stale.Count -gt 0) { Say "Closed $($stale.Count) leftover worker/dashboard process(es) from a previous run." }

# The node token AND the W5b black-box session file, so each demo shows clean
# workers with no stale "comeback" story from the last run.
'a', 'b', 'c' | ForEach-Object {
  Remove-Item "$env:TEMP\fyp-node-$_.json", "$env:TEMP\fyp-node-$_.json.session" -ErrorAction SilentlyContinue
}
Ok 'Clean slate: 0 nodes, 0 runs; fresh registration for all 3 workers.'

# ===========================================================================
#  PHASE 5 - BUILD AND LAUNCH.
# ===========================================================================
# ALWAYS built, never only when missing (2026-09-06). This used to skip the build if
# an image of that name existed anywhere on the machine, which was fine while the
# image's contents never mattered. They matter now: the workload imports `fyp_data`
# to open its dataset and to seal its results, so a machine carrying yesterday's
# `fyp-dummy:latest` would stage a demonstration in which EVERY job fails on an
# import -- minutes before a jury, for a reason nothing on screen would explain. With
# Docker's layer cache an unchanged image costs a second or two; that is the right
# price for never staging a stale one.
Say 'Building the workload image (fyp-dummy:latest)...'
$build = Invoke-Native 'docker' @('build', '-t', 'fyp-dummy:latest', $DummyDir)
if ($build.ExitCode -ne 0) { Die ("Image build failed:" + [Environment]::NewLine + "         " + (Get-Tail $build.Output)) }
Ok 'Workload image ready (rebuilt from workloads/dummy).'

# Capacity is SET, never left to the host's core count. Until 2026-09-08 this script
# set no AGENT_CAPACITY, so each worker declared os.cpu_count() -- on this laptop far
# more runs than a worker can keep current, and a number that would change with the
# machine we demonstrate on. Three, because the agent's log budget is worker-wide:
# MAX_LOG_BATCHES_PER_TICK = 3 posts per tick spent across runs in rotation
# (agent/agent.py), so with N runs a given run is visited once every ceil(N / 3) ticks.
# At the default 3 s tick that is 3 s at N=3 and 6 s at N=4, and NFR-4's target is a log
# line in the browser within 5 seconds. Four runs is the first value past it, so three
# is the largest capacity that keeps the demonstration inside the requirement the report
# publishes as Met (median 3.04 s, 3.02-3.12, n=20).
Say 'Opening 3 worker windows (node-a, node-b, node-c), capacity 3 each...'
foreach ($n in 'a', 'b', 'c') {
  $state = "$env:TEMP\fyp-node-$n.json"
  $inner = "`$host.UI.RawUI.WindowTitle='WORKER node-$n'; Set-Location '$Root'; " +
           "`$env:AGENT_CAPACITY='3'; " +
           "`$env:AGENT_STATE_FILE='$state'; & '$VenvPython' -m agent --server $Api$agentTls --name node-$n"
  Start-Process powershell -ArgumentList '-NoExit', '-Command', $inner | Out-Null
}

Say 'Opening the dashboard window (Vite dev server)...'
$innerWeb = "`$host.UI.RawUI.WindowTitle='DASHBOARD (web)'; Set-Location '$WebDir'; npm run dev"
Start-Process powershell -ArgumentList '-NoExit', '-Command', $innerWeb | Out-Null

# W6 gated /nodes behind a login, so we log in exactly as the browser does.
Say 'Logging in (W6 auth: admin) to read the node pool...'
$token = $null
foreach ($i in 1..15) {
  try {
    $login = Invoke-Api -Url "$Api/auth/login" -Method 'POST' `
                -Body '{"username":"admin","password":"fyp-admin"}' -TimeoutSec 3
    if ($login) { $token = $login.token }
    if ($token) { break }
  } catch { }
  Start-Sleep -Seconds 1
}
if (-not $token) { Die 'Could not log in (admin/fyp-admin). Check: docker compose logs control-plane' }
$auth = @{ Authorization = "Bearer $token" }

# Storage tiers (2026-09-04). The bootstrap admin is created already having accepted
# its limits -- it is made from this deployment's own environment by the person who
# configured the tiers -- so everything above this point works exactly as it did
# before. THIS block adds the second half of the story, which is the half the
# supervisor asked to see: a user who was put in a smaller tier by an admin and who
# has NOT agreed to anything yet. Log in as `researcher` / `fyp-researcher` in a
# private browser window and the submit form shows the two numbers and refuses to
# submit until the box is ticked.
#
# Best-effort on purpose: a demonstration must never fail to stage because a
# convenience account could not be made. A 409 means it is already there from a
# previous staging, which is success by another name.
Say 'Seeding a second user in the smaller storage tier (researcher/fyp-researcher)...'
try {
  Invoke-Api -Url "$Api/users" -Method 'POST' -Token $token -TimeoutSec 5 `
    -Body '{"username":"researcher","password":"fyp-researcher","tier":"limited"}' | Out-Null
  Write-Host '  created: researcher (tier `limited`, limits NOT yet accepted)' -ForegroundColor DarkGray
} catch {
  Write-Host '  researcher already exists (or could not be created) - continuing' -ForegroundColor DarkGray
}

Say 'Waiting for all 3 workers to come ONLINE...'
$online = 0
foreach ($i in 1..30) {
  $pool = Invoke-Api -Url "$Api/nodes" -Token $token -TimeoutSec 3
  if ($pool) { $online = @($pool | Where-Object { $_.online }).Count }
  if ($online -ge 3) { break }
  Start-Sleep -Seconds 1
}
if ($online -lt 3) { Die "Only $online/3 workers came online. Check the worker windows for errors, then re-run me." }
Ok "$online/3 workers ONLINE."

Say 'Waiting for the dashboard (:5173)...'
$ui = $false
foreach ($i in 1..40) {
  if (Test-UrlReachable -Url $DashUrl -TimeoutSec 3) { $ui = $true; break }
  Start-Sleep -Seconds 1
}
if ($ui) { Ok "Dashboard is up at $DashUrl" }
else     { Warn 'Dashboard not answering yet - give the DASHBOARD window a few more seconds, then refresh the browser.' }

Say 'Waiting for the database GUI (pgAdmin, :5050)...'
$pgui = $false
foreach ($i in 1..45) {
  try { Invoke-WebRequest 'http://localhost:5050' -TimeoutSec 2 -UseBasicParsing | Out-Null; $pgui = $true; break } catch { }
  Start-Sleep -Seconds 1
}
if ($pgui) {
  Ok 'Database GUI up at http://localhost:5050'
  if (-not $NoBrowser) { Start-Process 'http://localhost:5050' }
} else {
  Warn 'pgAdmin not answering yet - on the very first run it can take ~30s. Then open http://localhost:5050'
}

$sw.Stop()
$secs = [math]::Round($sw.Elapsed.TotalSeconds, 1)

Write-Host "`n============  READY  in $secs s  ============" -ForegroundColor Green
Write-Host "  Open the browser:   $DashUrl   (zoom 125-150%)" -ForegroundColor White
if ($TlsOn) {
  Write-Host "  Transport:          ENCRYPTED (https/wss, certificate verified)" -ForegroundColor Green
} else {
  Write-Host "  Transport:          PLAIN HTTP - not encrypted" -ForegroundColor Yellow
}
$leaseLive = (Invoke-Native 'docker' @('exec', 'fyp-control-plane-1', 'printenv', 'LEASE_TTL_S')).Output.Trim()
if ($leaseLive -eq "$DemoLeaseTtlS") {
  Write-Host "  Lease:              $leaseLive s (DEMO value; the shipped default is 60 s)" -ForegroundColor Green
} else {
  Write-Host "  Lease:              $leaseLive s - EXPECTED $DemoLeaseTtlS s FOR THE DEMO" -ForegroundColor Red
  Write-Host "                      A killed run will take ~$leaseLive s to come back, not ~$DemoLeaseTtlS s." -ForegroundColor Red
}
Write-Host "  Pool shows:         node-a / node-b / node-c  ONLINE, bars moving" -ForegroundColor White
Write-Host "  Submit values:      fyp-dummy:latest | Epochs 25 | lr 0.01 | batch 32 | adam | seed 42" -ForegroundColor White
Write-Host "  Target nodes:       tick ALL THREE" -ForegroundColor White
Write-Host "  Database GUI:       http://localhost:5050   (pgAdmin - already connected, no login)" -ForegroundColor White
Write-Host "                      Tree: FYP > FYP Postgres > Databases > fyp > Schemas > public > Tables" -ForegroundColor White
Write-Host "                      'Show me the DB' tour: open pgadmin\db_tour.sql in the Query Tool (after you submit)" -ForegroundColor White
Write-Host "  Kill a worker:      powershell -ExecutionPolicy Bypass -File scripts\kill_worker.ps1 node-b" -ForegroundColor White
Write-Host "                      (Rule 0b: Stop-Process from PowerShell. Never close the window.)" -ForegroundColor White
Write-Host "  To tear down after: powershell -ExecutionPolicy Bypass -File scripts\stop_demo.ps1`n" -ForegroundColor White
