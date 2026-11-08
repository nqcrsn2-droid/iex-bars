#!/usr/bin/env python3
"""
IEX HIST -> 1-minute bars, for the licensed (redistributable) data mode.

IEX Exchange posts the full TOPS feed for each trading day as a free download on a
T+1 basis (the day's file usually appears around 7 AM ET the next morning). No
registration is needed. Delayed IEX data may be redistributed under IEX's market data
policies. Attribution shown in the app: "Data provided for free by IEX. By accessing
or using IEX Historical Data, you agree to the IEX Historical Data Terms of Use."

The file is large (~16 GB gzip, ~70 GB raw), so this never stores it. It streams the
download through gunzip and pulls only the Trade Report messages ('T', 38 bytes) for
the symbols we need with one bytes regex. That works on the pcap-ng container without
parsing it, because every IEX-TP message is a 2-byte little-endian length followed by
the message, so a trade report always appears as  26 00 54 ...

TOPS 1.6 Trade Report (little-endian):
  0  type 'T' (0x54)          1  sale condition flags
  2  timestamp int64 ns UTC   10 symbol, 8 bytes, space padded
  18 size uint32              22 price int64, 4 implied decimals
  30 trade id int64
Flags: 0x80 intermarket sweep, 0x40 extended hours, 0x20 odd lot,
       0x10 trade-through exempt, 0x08 single-price cross.

Usage:
  python scripts/iex_hist.py 2026-11-06            # one day
  python scripts/iex_hist.py 2026-10-01:2026-11-06 # each weekday in a range
  python scripts/iex_hist.py --file X.pcap.gz 2026-11-06   # parse a local file
Writes data/iex/bars/YYYY-MM-DD.json.gz and updates data/iex/index.json.
"""
from __future__ import annotations

import gzip
import json
import os
import re
import shutil
import struct
import subprocess
import sys
import time as _time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import requests

ET = ZoneInfo("America/New_York")
ROOT = Path(__file__).resolve().parent.parent
BARS = Path(os.environ.get("IEX_BARS_DIR", ROOT / "data" / "iex" / "bars"))
HIST_API = "https://iextrading.com/api/1.0/hist?date={d:%Y%m%d}"
ATTRIBUTION = ("Data provided for free by IEX. By accessing or using IEX Historical Data, "
               "you agree to the IEX Historical Data Terms of Use.")

# Every symbol the licensed mode reads. ETFs stand in for futures, FX, rates and credit;
# single stocks are the index heavyweights the report already tracks.
SYMBOLS = sorted(set("""
QQQ SPY DIA IWM GLD SLV CPER USO UNG FXE FXB FXY FXA FXC UUP IEF SHY TLT IEI HYG LQD VXX IBIT ETHA
XLK XLF XLE SMH KRE
AAPL MSFT NVDA AMZN GOOGL GOOG META AVGO TSLA COST NFLX AMD PLTR BRK.B JPM LLY V XOM UNH MA WMT
ORCL HD PG JNJ ABBV BAC CRM CSCO ADBE PEP KO MRK TMUS INTU QCOM TXN AMAT MU LRCX ISRG BKNG PANW
""".split()))

CHUNK = 64 << 20
TAIL = 64          # bytes kept between chunks so a message split across reads is still found


def _pattern(symbols):
    syms = b"|".join(re.escape(s.encode().ljust(8)) for s in symbols)
    return re.compile(rb"\x26\x00\x54([\x00-\xff])([\x00-\xff]{8})(" + syms + rb")([\x00-\xff]{4})([\x00-\xff]{8})",
                      re.DOTALL)


class Bars:
    """1-minute OHLCV keyed by ET minute-of-day, plus regular-session open/close."""

    def __init__(self):
        self.m: dict[str, dict[int, list]] = {}
        self.n = 0

    def add(self, sym, ts_ns, px, size, flags):
        t = datetime.fromtimestamp(ts_ns / 1e9, timezone.utc).astimezone(ET)
        k = t.hour * 60 + t.minute
        b = self.m.setdefault(sym, {}).get(k)
        if b is None:
            self.m[sym][k] = [px, px, px, px, size, ts_ns, ts_ns]   # o h l c v first last
        else:
            if ts_ns < b[5]:
                b[0], b[5] = px, ts_ns
            if ts_ns >= b[6]:
                b[3], b[6] = px, ts_ns
            b[1] = max(b[1], px); b[2] = min(b[2], px); b[4] += size
        self.n += 1

    def to_json(self, d: date):
        out = {}
        for sym, mm in self.m.items():
            out[sym] = [[k, round(b[0], 4), round(b[1], 4), round(b[2], 4), round(b[3], 4), b[4]]
                        for k, b in sorted(mm.items())]
        return {"date": d.isoformat(), "source": "IEX HIST TOPS 1.6 (T+1)", "attribution": ATTRIBUTION,
                "tz": "America/New_York", "fields": ["minute_of_day_et", "open", "high", "low", "close", "volume"],
                "trades": self.n, "bars": out}


def scan(stream, symbols=SYMBOLS, bars: Bars | None = None, progress=True):
    """Read a raw (already decompressed) TOPS stream and collect trade reports."""
    pat = _pattern(symbols)
    bars = bars or Bars()
    buf = b""
    seen = 0
    t0 = _time.time()
    while True:
        chunk = stream.read(CHUNK)
        last = not chunk
        buf += chunk
        limit = len(buf) if last else len(buf) - TAIL
        pos = 0
        for m in pat.finditer(buf, 0, len(buf)):
            if m.start() >= limit:
                break
            flags = m.group(1)[0]
            ts = struct.unpack("<q", m.group(2))[0]
            sym = m.group(3).decode().strip()
            size = struct.unpack("<I", m.group(4))[0]
            px = struct.unpack("<q", m.group(5))[0] / 1e4
            if not (0 < px < 1e6 and 0 < size < 5e7 and 1.0e18 < ts < 4.1e18):
                continue      # a coincidental byte match, not a real trade report
            bars.add(sym, ts, px, size, flags)
            pos = m.end()
        seen += len(chunk)
        if last:
            break
        buf = buf[max(limit, pos):]
        if progress and seen // (4 << 30) != (seen - len(chunk)) // (4 << 30):
            print(f"  {seen / 1e9:.0f} GB scanned, {bars.n:,} trades, {seen / 1e6 / (_time.time() - t0):.0f} MB/s",
                  file=sys.stderr)
    return bars


def hist_link(d: date):
    r = requests.get(HIST_API.format(d=d), timeout=30)
    r.raise_for_status()
    js = r.json() or []
    if isinstance(js, dict):
        js = js.get(d.strftime("%Y%m%d"), [])
    tops = [x for x in js if x.get("feed") == "TOPS"]
    return tops[0] if tops else None


def _gunzip_cmd():
    return ["pigz", "-dc"] if shutil.which("pigz") else ["gzip", "-dc"]


def build_day(d: date, local_file: str | None = None, force=False):
    out = BARS / f"{d.isoformat()}.json.gz"
    if out.exists() and not force:
        print(f"{d}: already built", file=sys.stderr)
        return out
    if local_file:
        src = local_file
        print(f"{d}: parsing {src}", file=sys.stderr)
        p = subprocess.Popen(_gunzip_cmd() + [src], stdout=subprocess.PIPE)
        procs = [p]
    else:
        info = hist_link(d)
        if not info:
            print(f"{d}: no TOPS file (holiday, weekend, or not posted yet)", file=sys.stderr)
            return None
        print(f"{d}: streaming {int(info['size']) / 1e9:.1f} GB TOPS {info.get('version')}", file=sys.stderr)
        dl = subprocess.Popen(["curl", "-sSfL", "--retry", "5", "--retry-delay", "10", info["link"]],
                              stdout=subprocess.PIPE)
        p = subprocess.Popen(_gunzip_cmd(), stdin=dl.stdout, stdout=subprocess.PIPE)
        dl.stdout.close()
        procs = [dl, p]
    t0 = _time.time()
    bars = scan(p.stdout)
    rc = [x.wait() for x in procs]
    if any(rc):
        raise RuntimeError(f"{d}: download/decompress failed {rc}")
    BARS.mkdir(parents=True, exist_ok=True)
    js = bars.to_json(d)
    with gzip.open(out, "wt") as f:
        json.dump(js, f, separators=(",", ":"))
    idx_p = BARS.parent / "index.json"
    idx = json.loads(idx_p.read_text()) if idx_p.exists() else {}
    idx[d.isoformat()] = {"trades": bars.n, "symbols": len(js["bars"]),
                          "built": datetime.now(ET).isoformat(timespec="seconds")}
    idx_p.write_text(json.dumps(dict(sorted(idx.items())), indent=1))
    print(f"{d}: {bars.n:,} trades, {len(js['bars'])} symbols in {_time.time() - t0:.0f}s -> {out}", file=sys.stderr)
    return out


def _days(spec):
    if ":" in spec:
        a, b = (date.fromisoformat(x) for x in spec.split(":"))
    else:
        a = b = date.fromisoformat(spec)
    d = a
    while d <= b:
        if d.weekday() < 5:
            yield d
        d += timedelta(days=1)


def main(argv):
    local = None
    if "--file" in argv:
        i = argv.index("--file")
        local = argv[i + 1]
        argv = argv[:i] + argv[i + 2:]
    force = "--force" in argv
    argv = [a for a in argv if a != "--force"]
    if not argv or argv[0] in ("yesterday", "latest"):
        d = datetime.now(ET).date() - timedelta(days=1)
        while d.weekday() >= 5:
            d -= timedelta(days=1)
        spec = d.isoformat()
    else:
        spec = argv[0]
    fails = 0
    for d in _days(spec):
        try:
            build_day(d, local, force)
        except Exception as e:  # noqa: BLE001
            fails += 1
            print(f"{d}: FAILED {type(e).__name__} {e}", file=sys.stderr)
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
