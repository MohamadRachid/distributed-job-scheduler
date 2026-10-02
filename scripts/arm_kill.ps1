<#
  arm_kill.ps1 - opens a window with the kill command PRE-TYPED but NOT entered.
  Runbook T-2 ("the kill command is already typed and not entered").

  Rule 0b: the kill is Stop-Process from PowerShell, on a named machine.
  Never close the window. Never kill from Git Bash - that kills the MSYS job
  and leaves the real python.exe heart-beating, so the lease never expires and
  the recovery never happens while the room watches nothing.

  DELIBERATE, DO NOT "TIDY": no text in this script or its banner contains the
  substring "WORKER <name>". kill_worker.ps1 matches candidate windows on that
  exact substring inside the process command line, and this window's command
  line contains its own banner text. A banner saying "WORKER node-b" would make
  the kill script match and kill THIS window too. That is the same defect
  kill_worker.ps1's own first version had, when it matched and killed the shell
  that invoked it (recorded 2026-08-15).
#>
param([string]$Node = 'node-b')

$Root = Split-Path -Parent $PSScriptRoot     # derived, never hard-coded
$Cmd  = "powershell -ExecutionPolicy Bypass -File scripts\kill_worker.ps1 $Node"

$inner = @"
`$host.UI.RawUI.WindowTitle = '>>> ARMED: KILL $Node - PRESS ENTER <<<'
Set-Location '$Root'
Write-Host ''
Write-Host '  ============================================================' -ForegroundColor Red
Write-Host '     ARMED - KILL $Node' -ForegroundColor Red
Write-Host '  ============================================================' -ForegroundColor Red
Write-Host ''
Write-Host '  The command is typed below. It has NOT run.' -ForegroundColor White
Write-Host '  Press ENTER to fire it. Nothing else.' -ForegroundColor Yellow
Write-Host ''
Write-Host '  If the line is blank, press the UP arrow once, then ENTER.' -ForegroundColor DarkGray
Write-Host ''
Write-Host '  After it fires, expect:' -ForegroundColor White
Write-Host '    [OK] $Node is gone - both processes, verified not assumed.' -ForegroundColor DarkGray
Write-Host '    If it does NOT print [OK], the machine is not dead. Do not continue.' -ForegroundColor DarkGray
Write-Host '    ~15 s later the lease expires, the reaper marks the run LOST,' -ForegroundColor DarkGray
Write-Host '    and it reappears on another machine at attempt 2.' -ForegroundColor DarkGray
Write-Host ''
try { [Microsoft.PowerShell.PSConsoleReadLine]::AddToHistory('$Cmd') } catch { }
"@

Start-Process powershell -ArgumentList '-NoExit','-Command',$inner | Out-Null

# Pre-type the command into that window, without pressing Enter.
# Best-effort: it types into whatever is foreground, so do not click away for a second.
try {
    $wsh = New-Object -ComObject WScript.Shell
    $seen = $false
    for ($i = 0; $i -lt 40; $i++) {
        Start-Sleep -Milliseconds 250
        if ($wsh.AppActivate(">>> ARMED: KILL $Node - PRESS ENTER <<<")) { $seen = $true; break }
    }
    if ($seen) {
        Start-Sleep -Milliseconds 400
        $wsh.SendKeys($Cmd)          # no ~ , so no Enter is sent
        Write-Host "  [OK]   Armed and pre-typed: $Cmd" -ForegroundColor Green
    } else {
        Write-Host "  [note] Window opened but could not be focused to pre-type." -ForegroundColor Yellow
        Write-Host "         In that window press UP then ENTER - the command is in its history." -ForegroundColor Yellow
    }
} catch {
    Write-Host "  [note] Could not pre-type. Press UP then ENTER in the armed window." -ForegroundColor Yellow
}
