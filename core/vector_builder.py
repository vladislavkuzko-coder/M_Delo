from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import tempfile
import zipfile
from dataclasses import dataclass
from typing import Callable, Optional, List, Tuple

import urllib.request
import ssl
import urllib.error
import time


def _ssl_context() -> ssl.SSLContext:
    """SSL context that uses certifi when available.

    On some Windows setups Python may not have a working CA bundle, causing
    HTTPS downloads (GitHub releases) to fail with CERTIFICATE_VERIFY_FAILED.
    """
    try:
        import certifi  # type: ignore

        return ssl.create_default_context(cafile=certifi.where())
    except Exception:
        return ssl.create_default_context()


def _ps_run(cmd: str) -> str:
    """Run a PowerShell command and return stdout.

    Used as a last-resort downloader on Windows when Python's SSL trust store
    is broken (CERTIFICATE_VERIFY_FAILED). PowerShell uses the system
    certificate store, which is often configured correctly.
    """
    p = subprocess.run(
        [
            "powershell",
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-Command",
            cmd,
        ],
        capture_output=True,
        text=True,
    )
    if p.returncode != 0:
        raise ToolingError((p.stderr or p.stdout or "PowerShell download failed").strip())
    return (p.stdout or "").strip()


def _ps_download(url: str, dst: str) -> None:
    os.makedirs(os.path.dirname(dst), exist_ok=True)
    # Force TLS 1.2+ and use system cert store
    cmd = (
        "$ErrorActionPreference='Stop';"
        "[Net.ServicePointManager]::SecurityProtocol = "
        "[Net.SecurityProtocolType]::Tls12 -bor [Net.SecurityProtocolType]::Tls13;"
        f"Invoke-WebRequest -Uri '{url}' -OutFile '{dst}' -UseBasicParsing "
        "-Headers @{ 'User-Agent'='Registry/1.0' };"
    )
    _ps_run(cmd)


def _ps_head(url: str, timeout_sec: int = 5) -> bool:
    """Fast reachability check using Windows networking stack.

    Some CDNs are blocked in certain networks. We prefer to fail fast instead of
    hanging for minutes on a 0-byte download.
    """
    try:
        cmd = (
            "$ErrorActionPreference='Stop';"
            "[Net.ServicePointManager]::SecurityProtocol = "
            "[Net.SecurityProtocolType]::Tls12 -bor [Net.SecurityProtocolType]::Tls13;"
            f"Invoke-WebRequest -Uri '{url}' -Method Head -TimeoutSec {int(timeout_sec)} "
            "-UseBasicParsing -Headers @{ 'User-Agent'='Registry/1.0' } | Out-Null;"
            "'OK'"
        )
        out = _ps_run(cmd)
        return out.strip().upper().endswith('OK')
    except Exception:
        return False


def _ps_get_json(url: str) -> dict:
    cmd = (
        "$ErrorActionPreference='Stop';"
        "[Net.ServicePointManager]::SecurityProtocol = "
        "[Net.SecurityProtocolType]::Tls12 -bor [Net.SecurityProtocolType]::Tls13;"
        f"$r = Invoke-RestMethod -Uri '{url}' -Headers @{{'User-Agent'='Registry/1.0'}};"
        "$r | ConvertTo-Json -Depth 32"
    )
    return json.loads(_ps_run(cmd))


class ToolingError(RuntimeError):
    pass
def _geojson_bbox(path: str) -> Optional[tuple[float, float, float, float]]:
    """Return (west,south,east,north) bbox from a GeoJSON file."""
    try:
        with open(path, "r", encoding="utf-8") as f:
            gj = json.load(f)
    except Exception:
        return None

    # Accept Feature, FeatureCollection, or Geometry
    geoms = []
    if isinstance(gj, dict) and gj.get("type") == "FeatureCollection":
        for ft in gj.get("features", []) or []:
            if isinstance(ft, dict) and ft.get("geometry"):
                geoms.append(ft["geometry"])
    elif isinstance(gj, dict) and gj.get("type") == "Feature":
        if gj.get("geometry"):
            geoms.append(gj["geometry"])
    elif isinstance(gj, dict) and gj.get("type"):
        geoms.append(gj)

    coords = []

    def walk(c):
        if isinstance(c, (list, tuple)):
            if len(c) == 2 and all(isinstance(x, (int, float)) for x in c):
                coords.append((float(c[0]), float(c[1])))
            else:
                for it in c:
                    walk(it)

    for g in geoms:
        walk(g.get("coordinates"))

    if not coords:
        return None
    xs = [p[0] for p in coords]
    ys = [p[1] for p in coords]
    return (min(xs), min(ys), max(xs), max(ys))


def _tilemaker_zoom_args(tilemaker_exe: str, zoom_min: int, zoom_max: int) -> list[str]:
    """Return zoom CLI args for the installed tilemaker, if supported.

    Tilemaker CLI has changed across releases. We detect supported flags via --help.
    If no known flags are present, return an empty list and rely on profile defaults.
    """
    try:
        h = subprocess.run([tilemaker_exe, "--help"], capture_output=True, text=True, timeout=10)
        help_text = (h.stdout or "") + "\n" + (h.stderr or "")
    except Exception:
        help_text = ""

    ht = help_text.lower()
    if "--minzoom" in ht and "--maxzoom" in ht:
        return ["--minzoom", str(int(zoom_min)), "--maxzoom", str(int(zoom_max))]
    if "--minimum-zoom" in ht and "--maximum-zoom" in ht:
        return ["--minimum-zoom", str(int(zoom_min)), "--maximum-zoom", str(int(zoom_max))]
    return []


def _tilemaker_threads_args(tilemaker_exe: str, threads: int) -> list[str]:
    """Return CLI args to limit tilemaker threads if supported.

    Some Windows builds of tilemaker can be unstable with many threads; limiting
    to 1-2 threads improves stability at the cost of speed.
    """
    try:
        h = subprocess.run([tilemaker_exe, "--help"], capture_output=True, text=True, timeout=10)
        ht = ((h.stdout or "") + "\n" + (h.stderr or "")).lower()
    except Exception:
        ht = ""
    if "--threads" in ht:
        return ["--threads", str(int(threads))]
    return []



def _http_get_json(url: str) -> dict:
    req = urllib.request.Request(url, headers={"User-Agent": "Registry/1.0"})
    try:
        with urllib.request.urlopen(req, timeout=60, context=_ssl_context()) as r:
            return json.loads(r.read().decode("utf-8"))
    except Exception as e:
        if "CERTIFICATE_VERIFY_FAILED" in str(e):
            return _ps_get_json(url)
        raise


def _download(
    url: str,
    dst: str,
    progress: Optional[Callable[[str], None]] = None,
    *,
    timeout: int = 120,
    retries: int = 3,
) -> None:
    os.makedirs(os.path.dirname(dst), exist_ok=True)
    req = urllib.request.Request(url, headers={"User-Agent": "Registry/1.0"})

    last_err: Optional[Exception] = None
    for attempt in range(max(1, int(retries))):
        try:
            with urllib.request.urlopen(req, timeout=int(timeout), context=_ssl_context()) as r, open(dst, "wb") as f:
                total = int(r.headers.get("Content-Length") or 0)
                done = 0
                while True:
                    chunk = r.read(1024 * 256)
                    if not chunk:
                        break
                    f.write(chunk)
                    done += len(chunk)
                    if progress and total:
                        progress(f"Скачивание… {done/total*100:.1f}%")
            return
        except urllib.error.HTTPError as e:
            last_err = e
            # Retry on transient server errors
            if 500 <= int(getattr(e, "code", 0) or 0) < 600 and attempt < retries - 1:
                if progress:
                    progress(f"Ошибка сервера {e.code}, повтор…")
                time.sleep(1.5 * (attempt + 1))
                continue
            if "CERTIFICATE_VERIFY_FAILED" in str(e):
                break
            raise
        except Exception as e:
            last_err = e
            if "CERTIFICATE_VERIFY_FAILED" in str(e):
                break
            if attempt < retries - 1:
                if progress:
                    progress("Пробую снова…")
                time.sleep(1.5 * (attempt + 1))
                continue
            break

    # Fallback: PowerShell downloader (uses Windows cert store)
    if progress:
        progress("Скачивание через PowerShell…")
    try:
        _ps_download(url, dst)
        return
    except Exception as e2:
        raise last_err or e2


def _extract_zip(zip_path: str, dst_dir: str) -> None:
    os.makedirs(dst_dir, exist_ok=True)
    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(dst_dir)


def _find_file(root: str, pattern: str) -> Optional[str]:
    rx = re.compile(pattern, re.I)
    for dp, _dn, files in os.walk(root):
        for fn in files:
            if rx.search(fn):
                return os.path.join(dp, fn)
    return None


def _find_files(root: str, pattern: str) -> List[str]:
    rx = re.compile(pattern, re.I)
    out: List[str] = []
    for dp, _dn, files in os.walk(root):
        for fn in files:
            if rx.search(fn):
                out.append(os.path.join(dp, fn))
    return out


def _select_tilemaker_profile(tm_dir: str) -> Tuple[Optional[str], Optional[str]]:
    """Pick the best tilemaker config/process pair.

    Tilemaker bundles multiple example profiles (e.g. coastline) that are NOT
    suitable for rendering general OSM maps. We strongly prefer OpenMapTiles.
    """
    cfgs = _find_files(tm_dir, r"config.*\.json$")
    procs = _find_files(tm_dir, r"process.*\.lua$")

    def pick_contains(paths: List[str], needle: str) -> Optional[str]:
        needle = needle.lower()
        for p in paths:
            if needle in os.path.basename(p).lower():
                return p
        return None

    # Prefer OpenMapTiles profile if present
    cfg = pick_contains(cfgs, "openmaptiles")
    proc = pick_contains(procs, "openmaptiles")
    if cfg and proc:
        return cfg, proc

    # Fall back to generic config/process (avoid coastline)
    def not_coast(p: str) -> bool:
        return "coast" not in os.path.basename(p).lower()

    cfgs2 = [p for p in cfgs if not_coast(p)]
    procs2 = [p for p in procs if not_coast(p)]

    cfg = (cfgs2 or cfgs or [None])[0]
    proc = (procs2 or procs or [None])[0]
    return cfg, proc


def _ensure_pmtiles_exe(pm_dir: str) -> Optional[str]:
    """Locate pmtiles binary in extracted folder and normalize it to pmtiles.exe.

    GitHub release ZIPs may name the binary differently (e.g. pmtiles.exe,
    pmtiles-windows-amd64.exe, or pmtiles without extension). We accept any
    of these and copy it to <pm_dir>/pmtiles.exe.
    """
    candidates: List[str] = []
    for base, _dirs, files in os.walk(pm_dir):
        for fn in files:
            low = fn.lower()
            if low == "pmtiles.exe" or ("pmtiles" in low and low.endswith(".exe")) or low == "pmtiles":
                candidates.append(os.path.join(base, fn))

    if not candidates:
        return None

    # Prefer exact pmtiles.exe first
    candidates.sort(key=lambda p: (os.path.basename(p).lower() != "pmtiles.exe", len(p)))
    src = candidates[0]
    dst = os.path.join(pm_dir, "pmtiles.exe")
    os.makedirs(pm_dir, exist_ok=True)
    try:
        shutil.copy2(src, dst)
        return dst
    except Exception:
        return src


@dataclass
class Tools:
    tilemaker: str
    pmtiles: str
    osmium: Optional[str]
    tilemaker_config: str
    tilemaker_process: str


def ensure_planetiler(tools_dir: str, say: Callable[[str], None]) -> str:
    """Ensure planetiler.jar is available and return its path."""
    pdir = os.path.join(tools_dir, "planetiler")
    jar = os.path.join(pdir, "planetiler.jar")
    if os.path.exists(jar) and os.path.getsize(jar) > 1024 * 1024:
        return jar
    say("Скачиваю planetiler.jar…")
    url = "https://github.com/onthegomap/planetiler/releases/latest/download/planetiler.jar"
    _download(url, jar, progress=say)
    if not os.path.exists(jar):
        raise ToolingError("Не удалось скачать planetiler.jar")
    return jar


def _ensure_planetiler_sources(download_dir: str, say: Callable[[str], None]) -> None:
    """Prefetch auxiliary datasets Planetiler downloads on first run.

    In some networks, downloads from naciscdn.org may time out. Prefetching the
    Natural Earth SQLite package using our robust downloader (PowerShell fallback)
    improves reliability.
    """
    os.makedirs(download_dir, exist_ok=True)

    ne_dst = os.path.join(download_dir, "natural_earth_vector.sqlite.zip")
    # Full Natural Earth SQLite zip is very large (~423MB). Treat anything smaller as incomplete.
    MIN_NE_BYTES = 100 * 1024 * 1024
    if os.path.exists(ne_dst) and os.path.getsize(ne_dst) > MIN_NE_BYTES:
        return

    # Some networks/timezones/providers may block or throttle naciscdn.org.
    # Try several mirrors. Download to a temp file first to avoid leaving 0-byte files.
    # Prefer MapTiler mirror (used by planetiler-openmaptiles) when naciscdn is blocked.
    ne_urls = [
        "https://dev.maptiler.download/geodata/omt/natural_earth_vector.sqlite.zip",
        "https://naciscdn.org/naturalearth/packages/natural_earth_vector.sqlite.zip",
        "http://naciscdn.org/naturalearth/packages/natural_earth_vector.sqlite.zip",
        # Keep as a last-ditch fallback; validate size because GitHub "test resources" can be tiny.
        "https://github.com/onthegomap/planetiler/raw/main/planetiler-core/src/test/resources/natural_earth_vector.sqlite.zip",
    ]

    if os.path.exists(ne_dst) and os.path.getsize(ne_dst) == 0:
        try:
            os.remove(ne_dst)
        except OSError:
            pass

    say("Скачиваю Natural Earth (разовая зависимость)…")
    last_err: Optional[Exception] = None
    for i, url in enumerate(ne_urls, start=1):
        # Fail fast if the URL is blocked/unreachable in this network.
        if not _ps_head(url, timeout_sec=5):
            say(f"Источник {i}/{len(ne_urls)} недоступен (preflight). Пропускаю…")
            continue
        tmp_dst = ne_dst + ".part"
        try:
            if os.path.exists(tmp_dst):
                os.remove(tmp_dst)
            say(f"Источник {i}/{len(ne_urls)}: {url}")
            _download(url, tmp_dst, progress=say, timeout=3600, retries=10)
            sz = os.path.getsize(tmp_dst) if os.path.exists(tmp_dst) else 0
            if sz < MIN_NE_BYTES:
                raise ToolingError(
                    f"Скачанный файл слишком маленький ({sz} байт). Источник недоступен/ограничен или это не полный пакет Natural Earth."
                )
            os.replace(tmp_dst, ne_dst)
            last_err = None
            break
        except Exception as e:
            last_err = e
            try:
                if os.path.exists(tmp_dst):
                    os.remove(tmp_dst)
            except OSError:
                pass
            say(f"Не удалось скачать из источника {i}: {e}")
            continue

    if last_err is not None:
        raise ToolingError(
            "Не удалось скачать Natural Earth из всех источников. "
            "Возможна блокировка/прокси. Можно скачать файл вручную и положить в: "
            f"{ne_dst}"
        ) from last_err


def ensure_java(tools_dir: str, say: Callable[[str], None]) -> str:
    """Return a java executable.

    Prefer system Java if present. Otherwise download a portable JRE (Temurin 21)
    into tools/java and return its java.exe.
    """
    # 1) System java
    try:
        p = subprocess.run(["java", "-version"], capture_output=True, text=True)
        if p.returncode == 0:
            return "java"
    except Exception:
        pass

    jdir = os.path.join(tools_dir, "java")
    java_exe = _find_file(jdir, r"java\.exe$")
    if java_exe and os.path.exists(java_exe):
        return java_exe

    # 2) Download portable JRE
    say("Скачиваю Java Runtime (Temurin 21)…")

    # Primary: Adoptium direct binary endpoint (fast, no JSON)
    candidates: list[str] = [
        "https://api.adoptium.net/v3/binary/latest/21/ga/windows/x64/jre/hotspot/normal/eclipse",
        "https://api.adoptium.net/v3/binary/latest/21/ga/windows/x64/jre/hotspot/normal/eclipse?project=jdk",
    ]

    # Secondary: GitHub Temurin binaries (more stable for some networks)
    try:
        rel = _http_get_json("https://api.github.com/repos/adoptium/temurin21-binaries/releases/latest")
        assets = list(rel.get("assets", []) or [])
        for a in assets:
            name = str(a.get("name") or "")
            low = name.lower()
            if not low.endswith(".zip"):
                continue
            if "jre" not in low:
                continue
            if "windows" not in low:
                continue
            if "x64" not in low and "x86_64" not in low and "amd64" not in low:
                continue
            if "hotspot" not in low:
                continue
            url = a.get("browser_download_url")
            if url:
                candidates.append(url)
                break
    except Exception:
        pass

    zpath = os.path.join(jdir, "jre.zip")
    last: Optional[Exception] = None
    for url in candidates:
        try:
            _download(url, zpath, progress=say, timeout=600, retries=4)
            _extract_zip(zpath, jdir)
            java_exe = _find_file(jdir, r"java\.exe$")
            if java_exe:
                return java_exe
        except Exception as e:
            last = e
            continue
    raise ToolingError(f"Java скачалась, но java.exe не найден ({last})")


def ensure_tools(tools_dir: str, status: Optional[Callable[[str], None]] = None) -> Tools:
    """Download required CLI tools into tools_dir (once)."""

    tools_dir = os.path.abspath(tools_dir)
    os.makedirs(tools_dir, exist_ok=True)

    def say(msg: str):
        if status:
            status(msg)

    # tilemaker
    tm_dir = os.path.join(tools_dir, "tilemaker")
    tm_exe = _find_file(tm_dir, r"tilemaker\.exe$")
    cfg, proc = _select_tilemaker_profile(tm_dir)
    if not (tm_exe and cfg and proc):
        say("Скачиваю tilemaker…")
        rel = _http_get_json("https://api.github.com/repos/systemed/tilemaker/releases/latest")
        def _zip_has_good_tilemaker(zpath: str) -> bool:
            try:
                with zipfile.ZipFile(zpath, "r") as zf:
                    members = [m.replace("\\", "/") for m in zf.namelist()]
                # Prefer prebuilt archives that contain tilemaker.exe outside build trees.
                good = [
                    m
                    for m in members
                    if m.lower().endswith("tilemaker.exe")
                    and "relwithdebinfo" not in m.lower()
                    and "/build/" not in m.lower()
                ]
                if good:
                    return True
                # Fallback: accept any tilemaker.exe at all.
                return any(m.lower().endswith("tilemaker.exe") for m in members)
            except Exception:
                return False

        assets = list(rel.get("assets", []) or [])

        def _score(a) -> int:
            name = str(a.get("name") or "")
            low = name.lower()
            s = 0
            if low.endswith(".zip"):
                s += 10
            if "win" in low or "windows" in low:
                s += 10
            if "x64" in low or "amd64" in low or "x86_64" in low:
                s += 10
            if "source" in low:
                s -= 50
            if "debug" in low:
                s -= 5
            return -s

        assets.sort(key=_score)

        picked_url = None
        zpath = os.path.join(tools_dir, "tilemaker.zip")
        for a in assets:
            name = str(a.get("name") or "")
            low = name.lower()
            if not low.endswith(".zip"):
                continue
            if "source" in low:
                continue
            url = a.get("browser_download_url")
            if not url:
                continue
            try:
                _download(url, zpath, progress=say)
                if _zip_has_good_tilemaker(zpath):
                    picked_url = url
                    break
            except Exception:
                continue
        if not picked_url:
            raise ToolingError("Не найден подходящий Windows ZIP для tilemaker")
        if os.path.exists(tm_dir):
            shutil.rmtree(tm_dir, ignore_errors=True)
        _extract_zip(zpath, tm_dir)
        tm_exe = _find_file(tm_dir, r"tilemaker\.exe$")
        cfg, proc = _select_tilemaker_profile(tm_dir)
        if not (tm_exe and cfg and proc):
            raise ToolingError("tilemaker скачался, но не найдены tilemaker.exe/config/process")

    # pmtiles
    pm_dir = os.path.join(tools_dir, "pmtiles")
    pm_exe = _find_file(pm_dir, r"pmtiles\.exe$")
    if not pm_exe:
        say("Скачиваю pmtiles…")
        rel = _http_get_json("https://api.github.com/repos/protomaps/go-pmtiles/releases/latest")
        asset = None
        for a in rel.get("assets", []):
            name = str(a.get("name") or "")
            if "win" in name.lower() and name.lower().endswith(".zip"):
                asset = a
                break
        if not asset:
            raise ToolingError("Не найден Windows ZIP для pmtiles")
        url = asset.get("browser_download_url")
        zpath = os.path.join(tools_dir, "pmtiles.zip")
        _download(url, zpath, progress=say)
        if os.path.exists(pm_dir):
            shutil.rmtree(pm_dir, ignore_errors=True)
        _extract_zip(zpath, pm_dir)
        pm_exe = _find_file(pm_dir, r"pmtiles\.exe$")
        if not pm_exe:
            # Normalize different binary names to pmtiles.exe
            pm_exe = _ensure_pmtiles_exe(pm_dir)
        if not pm_exe:
            # Provide a more helpful message for troubleshooting
            raise ToolingError("pmtiles скачался, но бинарник pmtiles не найден в архиве")

    # Ensure path points to an exe we can execute
    if pm_exe and os.path.basename(pm_exe).lower() != "pmtiles.exe":
        norm = _ensure_pmtiles_exe(pm_dir)
        if norm:
            pm_exe = norm

    # osmium-tool
    osm_dir = os.path.join(tools_dir, "osmium")
    osm_exe = _find_file(osm_dir, r"osmium\.exe$")
    if not osm_exe:
        say("Скачиваю osmium-tool…")
        rel = _http_get_json("https://api.github.com/repos/osmcode/osmium-tool/releases/latest")
        asset = None
        for a in rel.get("assets", []):
            name = str(a.get("name") or "")
            if "win" in name.lower() and name.lower().endswith(".zip"):
                asset = a
                break
        if not asset:
            say("⚠ Не найден Windows ZIP для osmium-tool. Обрезка по полигону будет недоступна, использую bbox из GeoJSON.")
            osm_exe = None
        else:
            url = asset.get("browser_download_url")
            zpath = os.path.join(tools_dir, "osmium.zip")
            _download(url, zpath, progress=say)
            if os.path.exists(osm_dir):
                shutil.rmtree(osm_dir, ignore_errors=True)
            _extract_zip(zpath, osm_dir)
            osm_exe = _find_file(osm_dir, r"osmium\.exe$")
            if not osm_exe:
                raise ToolingError("osmium-tool скачался, но osmium.exe не найден")

    return Tools(tilemaker=tm_exe, pmtiles=pm_exe, osmium=osm_exe, tilemaker_config=cfg, tilemaker_process=proc)


def build_pmtiles(
    pbf_path: str,
    geojson_mask: Optional[str],
    out_pmtiles: str,
    zoom_min: int,
    zoom_max: int,
    tools_dir: str,
    status: Optional[Callable[[str], None]] = None,

) -> str:
    """Build PMTiles from a PBF.

    On Windows, some tilemaker builds crash with exit code 3221226505.
    To provide a stable "как Яндекс" experience, we use Planetiler (Java)
    to generate vector tiles (PMTiles) when available.
    """

    def say(msg: str):
        if status:
            status(msg)

    # We intentionally do not call ensure_tools() here.
    # Planetiler writes PMTiles directly and is more stable on Windows than tilemaker.

    pbf_path = os.path.abspath(pbf_path)
    geojson_mask = os.path.abspath(geojson_mask) if geojson_mask else ""
    out_pmtiles = os.path.abspath(out_pmtiles)
    os.makedirs(os.path.dirname(out_pmtiles), exist_ok=True)

    def _has_non_ascii(s: str) -> bool:
        try:
            s.encode("ascii")
            return False
        except UnicodeEncodeError:
            return True

    def _stage_file(src: str, dst: str) -> str:
        """Stage a file into a safe ASCII temp path.

        Some Windows CLI tools (notably some C++ builds) can crash on non-ASCII paths.
        Prefer a hardlink to avoid copying large PBFs when possible.
        """
        if os.path.exists(dst):
            return dst
        os.makedirs(os.path.dirname(dst), exist_ok=True)
        try:
            os.link(src, dst)
            return dst
        except Exception:
            pass
        shutil.copy2(src, dst)
        return dst

    with tempfile.TemporaryDirectory(prefix="registry_vec_") as td:
        # Stage inputs into ASCII-only paths to avoid issues with some external tools
        safe_pbf = pbf_path
        if _has_non_ascii(pbf_path):
            say("⚠ Путь к PBF содержит не-ASCII символы. Использую временную копию для сборки…")
            safe_pbf = _stage_file(pbf_path, os.path.join(td, "input.osm.pbf"))

        safe_geojson = geojson_mask
        if geojson_mask and _has_non_ascii(geojson_mask):
            say("⚠ Путь к GeoJSON содержит не-ASCII символы. Использую временную копию…")
            safe_geojson = _stage_file(geojson_mask, os.path.join(td, "mask.geojson"))

        bbox = None
        if safe_geojson and os.path.exists(safe_geojson):
            try:
                bbox = _geojson_bbox(safe_geojson)
            except Exception:
                bbox = None

        # Use Planetiler (Java) for stable vector tile generation.
        java_exe = ensure_java(tools_dir, say)
        planetiler_jar = ensure_planetiler(tools_dir, say)

        say("Собираю векторную карту (planetiler)…")

        # Store full logs on disk so the UI can show a short message.
        materials_dir = os.path.abspath(os.path.join(tools_dir, os.pardir))
        logs_dir = os.path.join(materials_dir, "logs")
        os.makedirs(logs_dir, exist_ok=True)
        log_path = os.path.join(logs_dir, "planetiler_build.log")

        # Keep Planetiler downloads in a stable per-app folder, and prefetch
        # known large dependencies to avoid in-process timeouts.
        sources_dir = os.path.join(materials_dir, "cache", "planetiler_sources")
        try:
            _ensure_planetiler_sources(sources_dir, say)
        except Exception as _e:
            # If prefetch fails, Planetiler will try again. We'll still run it.
            say(f"⚠ Не удалось заранее скачать Natural Earth: {_e}")
        cmd = [
            java_exe,
            "-Xmx4g",
            # Planetiler downloads auxiliary datasets on first run. Some networks are slow/unstable,
            # so use large timeouts to avoid aborting the whole build.
            "-Dsun.net.client.defaultConnectTimeout=600000",
            "-Dsun.net.client.defaultReadTimeout=3600000",
            "-jar",
            planetiler_jar,
            "--download",
            f"--download_dir={sources_dir}",
            # planetiler expects ISO-8601 durations (java.time.Duration), e.g. PT10M
            "--http_timeout=PT10M",
            "--http_retries=20",
            "--http_retry_wait=PT10S",
            f"--osm-path={safe_pbf}",
            f"--output={out_pmtiles}",
            "--force",
            f"--maxzoom={zoom_max}",
            f"--render_maxzoom={zoom_max}",
        ]
        if bbox:
            w, s, e, n = bbox
            cmd.append(f"--bounds={w},{s},{e},{n}")

        # Create the log file immediately and stream output so the user can see progress.
        try:
            with open(log_path, 'w', encoding='utf-8', errors='replace') as f:
                f.write('COMMAND\n' + ' '.join(cmd) + '\n\n')
                f.write('OUTPUT\n')
                f.flush()
        except Exception:
            pass

        say(f'Лог: {log_path}')

        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )

        tail = []

        def add_tail(line: str):
            tail.append(line)
            if len(tail) > 60:
                del tail[:-60]

        with open(log_path, 'a', encoding='utf-8', errors='replace') as lf:
            assert proc.stdout is not None
            for raw in proc.stdout:
                line = raw.rstrip('\n')
                add_tail(line)
                try:
                    lf.write(line + '\n')
                    lf.flush()
                except Exception:
                    pass
                if line.strip():
                    msg = line.strip()
                    if len(msg) > 200:
                        msg = msg[:200] + '…'
                    say(msg)

        rc = proc.wait()
        if rc != 0:
            excerpt = '\n'.join(tail).strip()
            if len(excerpt) > 1600:
                excerpt = excerpt[-1600:]
            raise ToolingError(f"planetiler завершился с ошибкой (код {rc}).\n{excerpt}\n\nПолный лог: {log_path}")
    say("Готово")
    return out_pmtiles
