"""Best-effort machine identity + usage sampling (the rich dashboard, W4).

The pool is heterogeneous by design, so every probe here is allowed to fail:
whatever we can detect goes in, whatever we can't is simply omitted (the
control plane stores the dict as-is; the UI shows a dash). NOTHING in the
scheduler depends on these values — matching still runs on the declared specs
(cpu_cores / has_gpu / ram_mb / capacity). This module only feeds the screen.

Two entry points:
  collect_hw_specs(docker_version)  -> static identity dict, once at startup
  collect_usage()                   -> {cpu_pct, ram_pct, ...}, every heartbeat
"""

import logging
import os
import platform
import shutil
import subprocess
import sys

import psutil

log = logging.getLogger("agent.sysinfo")

_SUBPROC_TIMEOUT_S = 5


def _run(cmd: list[str]) -> str | None:
    """Run a probe command; None on any failure (missing tool, timeout, error)."""
    try:
        out = subprocess.run(
            cmd, capture_output=True, text=True, timeout=_SUBPROC_TIMEOUT_S
        )
        return out.stdout.strip() or None if out.returncode == 0 else None
    except (OSError, subprocess.SubprocessError):
        return None


def _powershell(expr: str) -> str | None:
    return _run(["powershell", "-NoProfile", "-Command", expr])


# --- identity probes (each returns None when it can't know) -------------------


def _cpu_name() -> str | None:
    if sys.platform == "win32":
        try:
            import winreg

            with winreg.OpenKey(
                winreg.HKEY_LOCAL_MACHINE,
                r"HARDWARE\DESCRIPTION\System\CentralProcessor\0",
            ) as key:
                return str(winreg.QueryValueEx(key, "ProcessorNameString")[0]).strip()
        except OSError:
            return None
    if sys.platform.startswith("linux"):
        try:
            with open("/proc/cpuinfo", encoding="utf-8") as f:
                for line in f:
                    if line.lower().startswith("model name"):
                        return line.split(":", 1)[1].strip()
        except OSError:
            return None
    if sys.platform == "darwin":
        return _run(["sysctl", "-n", "machdep.cpu.brand_string"])
    return None


def _machine_model() -> str | None:
    if sys.platform == "win32":
        val = _powershell(
            "$c = Get-CimInstance Win32_ComputerSystem; "
            "Write-Output ($c.Manufacturer + ' ' + $c.Model)"
        )
        return val
    if sys.platform.startswith("linux"):
        try:
            base = "/sys/devices/virtual/dmi/id"
            with open(f"{base}/sys_vendor", encoding="utf-8") as f:
                vendor = f.read().strip()
            with open(f"{base}/product_name", encoding="utf-8") as f:
                product = f.read().strip()
            return f"{vendor} {product}".strip() or None
        except OSError:
            return None
    return None


def _ram_mhz() -> int | None:
    if sys.platform == "win32":
        val = _powershell(
            "(Get-CimInstance Win32_PhysicalMemory | "
            "Measure-Object -Property Speed -Maximum).Maximum"
        )
        try:
            return int(float(val)) if val else None
        except ValueError:
            return None
    return None  # Linux needs root/dmidecode — omit rather than guess


def _gpu_name() -> str | None:
    # NVIDIA first (also unlocks usage sampling), then the generic Windows probe
    # so integrated GPUs (Intel/AMD) still show by name.
    val = _run(["nvidia-smi", "--query-gpu=name", "--format=csv,noheader"])
    if val:
        return val.splitlines()[0].strip()
    if sys.platform == "win32":
        val = _powershell(
            "(Get-CimInstance Win32_VideoController | "
            "Select-Object -First 1 -ExpandProperty Name)"
        )
        return val
    return None


def _disk_total_gb() -> float | None:
    try:
        return round(psutil.disk_usage(os.path.abspath(os.sep)).total / 1024**3, 1)
    except OSError:
        return None


_HAS_NVIDIA_SMI = shutil.which("nvidia-smi") is not None


def collect_hw_specs(docker_version: str | None = None) -> dict:
    """Static identity, collected once at startup. Omits what it can't detect."""
    specs = {
        "cpu_name": _cpu_name(),
        "cpu_cores_physical": psutil.cpu_count(logical=False),
        "cpu_threads": psutil.cpu_count(logical=True),
        "gpu_name": _gpu_name(),
        "ram_mhz": _ram_mhz(),
        "machine_model": _machine_model(),
        "os": platform.platform(),
        "hostname": platform.node(),
        "python_version": platform.python_version(),
        "docker_version": docker_version,
        "disk_total_gb": _disk_total_gb(),
    }
    return {k: v for k, v in specs.items() if v is not None}


# Prime the CPU counter: psutil's first no-interval call always returns 0.0
# (it measures *since the previous call*), so take the throwaway reading now.
psutil.cpu_percent(interval=None)


def collect_battery() -> dict | None:
    """The node black box (W5b): battery % + charging state, best-effort. Returns
    None on a desktop / server (no battery) — psutil.sensors_battery() is None
    there — so the field is simply omitted and never guessed."""
    try:
        bat = psutil.sensors_battery()
    except Exception:  # noqa: BLE001 - identity is best-effort, never fatal
        return None
    if bat is None:
        return None
    return {"battery_pct": round(float(bat.percent), 1), "battery_charging": bool(bat.power_plugged)}


def collect_usage() -> dict:
    """Current usage sample for one heartbeat. Cheap by construction: cpu_pct is
    measured since the previous heartbeat's call (no blocking interval)."""
    usage = {
        "cpu_pct": psutil.cpu_percent(interval=None),
        "ram_pct": psutil.virtual_memory().percent,
    }
    try:
        usage["disk_pct"] = psutil.disk_usage(os.path.abspath(os.sep)).percent
    except OSError:
        pass
    if _HAS_NVIDIA_SMI:
        val = _run(
            ["nvidia-smi", "--query-gpu=utilization.gpu,memory.used,memory.total",
             "--format=csv,noheader,nounits"]
        )
        if val:
            try:
                gpu_pct, mem_used, mem_total = (
                    float(x.strip()) for x in val.splitlines()[0].split(",")
                )
                usage["gpu_pct"] = gpu_pct
                if mem_total:
                    usage["gpu_mem_pct"] = round(100.0 * mem_used / mem_total, 1)
            except (ValueError, ZeroDivisionError):
                pass
    return usage
