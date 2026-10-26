#!/usr/bin/env python3
"""Download every public CrossPump source into OUT/crosspump/ (OUT = $CQ_OUTPUT_DIR in a cq job).
Writes OUT/fetch_manifest.json and an n/N line per step to OUT/progress.log. Public sources only; nothing is generated.
    python scripts/xp_fetch_data.py --out $CQ_OUTPUT_DIR [--skip-klines]
"""
from __future__ import annotations
import argparse, concurrent.futures as cf, csv, io, json, os, subprocess, time, urllib.request, zipfile
from datetime import datetime, timedelta

GIT = {"hu_pd": "https://github.com/Bayi-Hu/Pump-and-Dump-Detection-on-Cryptocurrency", "sapienza_pd": "https://github.com/SystemsLab-Sapienza/pump-and-dump-dataset",
       "xu_pumpdump": "https://github.com/xujiahuayz/pumpdump", "mirtaheri": "https://github.com/Mehrnoom/Cryptocurrency-Pump-Dump",
       "nghiem": "https://github.com/hnghiem-nlp/Pump_and_Dump_Crypto", "moura": "https://github.com/Filipey/investigating-cryptocurrency-manipulation-icwsm2027",
       "ardia": "https://github.com/ArdiaD/PumpDump", "rugpull_defender": "https://github.com/mohamadpishdar/RugPull-Defender"}
ZENODO = [("memechain/data.zip", "18246856", "data.zip"), ("memechain/README.md", "18246856", "README.md"),
          ("victor_dex/EtherDollarPrice.csv", "4540223", "EtherDollarPrice.csv"), ("victor_dex/token_decimals.json", "4540223", "token_decimals.json"),
          ("victor_dex/EtherDeltaTrades.csv", "4540223", "EtherDeltaTrades.csv"), ("victor_dex/IDEXTrades.csv", "4540223", "IDEXTrades.csv"),
          ("pumpsense/icbc_2026_reproducibility-v1.1.zip", "19616802", "AhmedMahrous00/icbc_2026_reproducibility-v1.1.zip")]


def main():
    ap = argparse.ArgumentParser(); ap.add_argument("--out", required=True); ap.add_argument("--skip-klines", action="store_true"); ap.add_argument("--workers", type=int, default=8)
    a = ap.parse_args()
    R = os.path.join(a.out, "crosspump"); os.makedirs(R, exist_ok=True)
    man = {"git": {}, "zenodo": {}, "klines": {}}
    steps = len(GIT) + len(ZENODO) + 1; k = 0
    prog = open(os.path.join(a.out, "progress.log"), "a")
    tick = lambda msg: (prog.write(f"{k}/{steps} {msg}\n"), prog.flush())
    for name, url in GIT.items():
        d = os.path.join(R, name)
        rc = 0 if os.path.isdir(os.path.join(d, ".git")) else subprocess.call(["git", "clone", "-q", "--depth", "1", url, d])
        man["git"][name] = {"url": url, "rc": rc, "commit": subprocess.run(["git", "-C", d, "rev-parse", "HEAD"], capture_output=True, text=True).stdout.strip() if rc == 0 else None}
        k += 1; tick(f"git {name} rc={rc}")
    for rel, rec, fname in ZENODO:
        dst = os.path.join(R, rel); os.makedirs(os.path.dirname(dst), exist_ok=True)
        url = f"https://zenodo.org/records/{rec}/files/{fname}?download=1"
        if not (os.path.exists(dst) and os.path.getsize(dst) > 0):
            rc = subprocess.call(["curl", "-sS", "-L", "--retry", "5", "-C", "-", "-o", dst, url])
        else:
            rc = 0
        man["zenodo"][rel] = {"record": rec, "rc": rc, "bytes": os.path.getsize(dst) if os.path.exists(dst) else 0}
        k += 1; tick(f"zenodo {rel} rc={rc}")
    if not a.skip_klines:
        man["klines"] = fetch_klines(R, a.workers)
    k += 1; tick(f"klines {json.dumps({s: sum(1 for v in man['klines'].values() if v == s) for s in set(man['klines'].values())})}")
    json.dump(man, open(os.path.join(a.out, "fetch_manifest.json"), "w"), indent=1)
    prog.write("done\n"); prog.close()


def fetch_klines(R: str, workers: int):
    """Binance 1-minute klines (data.binance.vision) for every Binance BTC-pair pump in the Hu, Sapienza and Ardia tables, +-1 month."""
    pairs = set()
    def months(dt):
        for d in (-30, 0, 30):
            m = dt + timedelta(days=d); yield f"{m.year:04d}-{m.month:02d}"
    try:
        for r in csv.DictReader(open(os.path.join(R, "hu_pd/Data/Telegram/Labeled/PD_logs_cleaned.txt")), delimiter="\t"):
            if r["exchange"].strip().lower() == "binance" and r["pair"].strip().upper() == "BTC":
                for m in months(datetime.fromisoformat(r["timestamp"].strip().replace("+00:00", ""))): pairs.add((r["coin"].strip().upper(), m))
        for r in csv.DictReader(open(os.path.join(R, "sapienza_pd/pump_telegram.csv"))):
            if r["exchange"].strip().lower() == "binance":
                for m in months(datetime.strptime(r["date"].strip(), "%Y-%m-%d")): pairs.add((r["symbol"].strip().upper(), m))
        for r in csv.DictReader(open(os.path.join(R, "ardia/data/list_pd_events.csv"))):
            for m in months(datetime.strptime(r["Date"].strip()[:10], "%Y-%m-%d")): pairs.add((r["Currency"].strip().upper(), m))
    except FileNotFoundError as e:
        return {"error": str(e)}
    out = os.path.join(R, "binance_klines"); os.makedirs(out, exist_ok=True)
    def get(sm):
        sym, m = sm; name = f"{sym}BTC-1m-{m}"; dst = os.path.join(out, name + ".csv")
        if os.path.exists(dst): return name, "ok"
        url = f"https://data.binance.vision/data/spot/monthly/klines/{sym}BTC/1m/{name}.zip"
        for attempt in range(3):
            try:
                with urllib.request.urlopen(url, timeout=90) as resp: data = resp.read()
                z = zipfile.ZipFile(io.BytesIO(data)); open(dst, "wb").write(z.read(z.namelist()[0])); return name, "ok"
            except urllib.error.HTTPError as e:
                if e.code == 404: return name, "404"
            except Exception:
                time.sleep(2 * (attempt + 1))
        return name, "error"
    res = {}
    with cf.ThreadPoolExecutor(workers) as ex:
        for name, st in ex.map(get, sorted(pairs)): res[name] = st
    json.dump(res, open(os.path.join(out, "manifest.json"), "w"))
    return res


if __name__ == "__main__":
    main()
