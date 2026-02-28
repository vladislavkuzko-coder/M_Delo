from __future__ import annotations

import os
import shutil
import subprocess
import tempfile
import urllib.request
import zipfile
import ssl
import subprocess


def _ps_download(url: str, dst: str) -> None:
    os.makedirs(os.path.dirname(dst), exist_ok=True)
    cmd = (
        "$ErrorActionPreference='Stop';"
        "[Net.ServicePointManager]::SecurityProtocol = "
        "[Net.SecurityProtocolType]::Tls12 -bor [Net.SecurityProtocolType]::Tls13;"
        f"Invoke-WebRequest -Uri '{url}' -OutFile '{dst}' -UseBasicParsing "
        "-Headers @{ 'User-Agent'='Registry/1.0' };"
    )
    p = subprocess.run(
        ["powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-Command", cmd],
        capture_output=True,
        text=True,
    )
    if p.returncode != 0:
        raise RuntimeError((p.stderr or p.stdout or "PowerShell download failed").strip())


def _ssl_context() -> ssl.SSLContext:
    try:
        import certifi  # type: ignore

        return ssl.create_default_context(cafile=certifi.where())
    except Exception:
        return ssl.create_default_context()


def _download(url: str, dst: str) -> None:
    os.makedirs(os.path.dirname(dst), exist_ok=True)
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Registry/1.0"})
        with urllib.request.urlopen(req, context=_ssl_context()) as r, open(dst, "wb") as f:
            shutil.copyfileobj(r, f)
    except Exception as e:
        if "CERTIFICATE_VERIFY_FAILED" in str(e):
            _ps_download(url, dst)
            return
        raise


def ensure_tools(tools_dir: str) -> dict:
    """Ensure required CLI tools exist.

    Downloads (one time) into tools_dir:
    - osmium-tool (for polygon clip)
    - tilemaker (build vector mbtiles)
    - pmtiles (convert mbtiles->pmtiles)

    Returns dict with executable paths and resource files.
    """
    os.makedirs(tools_dir, exist_ok=True)
    bin_dir = os.path.join(tools_dir, "bin")
    res_dir = os.path.join(tools_dir, "tilemaker_resources")
    os.makedirs(bin_dir, exist_ok=True)
    os.makedirs(res_dir, exist_ok=True)

    # NOTE: URLs may change with new releases. If download fails, update URLs.
    osmium_exe = os.path.join(bin_dir, "osmium.exe")
    if not os.path.exists(osmium_exe):
        # osmium-tool windows builds
        osmium_zip = os.path.join(bin_dir, "osmium.zip")
        _download(
            "https://github.com/osmcode/osmium-tool/releases/download/v1.16.0/osmium-tool-1.16.0-win64.zip",
            osmium_zip,
        )
        with zipfile.ZipFile(osmium_zip, "r") as z:
            z.extractall(bin_dir)
        # try to locate exe
        for root, _, files in os.walk(bin_dir):
            if "osmium.exe" in files:
                shutil.copy2(os.path.join(root, "osmium.exe"), osmium_exe)
                break

    tilemaker_exe = os.path.join(bin_dir, "tilemaker.exe")
    if not os.path.exists(tilemaker_exe):
        tm_zip = os.path.join(bin_dir, "tilemaker.zip")
        _download(
            "https://github.com/systemed/tilemaker/releases/download/v2.4.0/tilemaker-windows-amd64.zip",
            tm_zip,
        )
        with zipfile.ZipFile(tm_zip, "r") as z:
            z.extractall(bin_dir)
        # locate tilemaker.exe
        for root, _, files in os.walk(bin_dir):
            if "tilemaker.exe" in files:
                shutil.copy2(os.path.join(root, "tilemaker.exe"), tilemaker_exe)
                break

    pmtiles_exe = os.path.join(bin_dir, "pmtiles.exe")
    if not os.path.exists(pmtiles_exe):
        pm_zip = os.path.join(bin_dir, "pmtiles.zip")
        _download(
            "https://github.com/protomaps/go-pmtiles/releases/download/v1.17.0/pmtiles_1.17.0_windows_amd64.zip",
            pm_zip,
        )
        with zipfile.ZipFile(pm_zip, "r") as z:
            z.extractall(bin_dir)
        # binary name might be pmtiles.exe already
        for root, _, files in os.walk(bin_dir):
            for fn in files:
                if fn.lower() == "pmtiles.exe":
                    shutil.copy2(os.path.join(root, fn), pmtiles_exe)
                    break

    # tilemaker openmaptiles resources
    cfg = os.path.join(res_dir, "config-openmaptiles.json")
    proc = os.path.join(res_dir, "process-openmaptiles.lua")
    if not os.path.exists(cfg):
        _download(
            "https://raw.githubusercontent.com/systemed/tilemaker/master/resources/config-openmaptiles.json",
            cfg,
        )
    if not os.path.exists(proc):
        _download(
            "https://raw.githubusercontent.com/systemed/tilemaker/master/resources/process-openmaptiles.lua",
            proc,
        )

    return {
        "osmium": osmium_exe,
        "tilemaker": tilemaker_exe,
        "pmtiles": pmtiles_exe,
        "tm_config": cfg,
        "tm_process": proc,
    }


def build_pmtiles(
    *,
    pbf_path: str,
    out_pmtiles: str,
    tools_dir: str,
    geojson_mask: str | None = None,
    min_zoom: int = 5,
    max_zoom: int = 14,
) -> None:
    tools = ensure_tools(tools_dir)
    work = tempfile.mkdtemp(prefix="reg_map_")
    try:
        src_pbf = pbf_path
        if geojson_mask:
            clipped = os.path.join(work, "clipped.pbf")
            subprocess.check_call([
                tools["osmium"],
                "extract",
                "-p",
                geojson_mask,
                src_pbf,
                "-o",
                clipped,
                "--overwrite",
            ])
            src_pbf = clipped

        out_mbtiles = os.path.join(work, "out.mbtiles")
        subprocess.check_call([
            tools["tilemaker"],
            "--input",
            src_pbf,
            "--output",
            out_mbtiles,
            "--config",
            tools["tm_config"],
            "--process",
            tools["tm_process"],
            "--minimum-zoom",
            str(min_zoom),
            "--maximum-zoom",
            str(max_zoom),
        ])

        os.makedirs(os.path.dirname(out_pmtiles), exist_ok=True)
        tmp_pm = os.path.join(work, "out.pmtiles")
        subprocess.check_call([
            tools["pmtiles"],
            "convert",
            out_mbtiles,
            tmp_pm,
        ])
        shutil.copy2(tmp_pm, out_pmtiles)
    finally:
        shutil.rmtree(work, ignore_errors=True)
