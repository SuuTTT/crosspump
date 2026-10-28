#!/usr/bin/env python3
"""Download every public CrossPump source into OUT/crosspump/ (OUT = $CQ_OUTPUT_DIR in a cq job).
Writes OUT/fetch_manifest.json and an n/N line per step to OUT/progress.log. Public sources only; nothing is generated.
    python scripts/xp_fetch_data.py --out $CQ_OUTPUT_DIR [--skip-klines]
"""
from __future__ import annotations
import argparse, concurrent.futures as cf, csv, io, json, os, subprocess, time, urllib.request, zipfile
from datetime import datetime, timedelta

GIT = {"ardia": "https://github.com/ArdiaD/PumpDump", "sapienza_pd": "https://github.com/SystemsLab-Sapienza/pump-and-dump-dataset",
       "moura": "https://github.com/Filipey/investigating-cryptocurrency-manipulation-icwsm2027", "nghiem": "https://github.com/hnghiem-nlp/Pump_and_Dump_Crypto",
       "xu_pumpdump": "https://github.com/xujiahuayz/pumpdump", "rugpull_defender": "https://github.com/mohamadpishdar/RugPull-Defender",
       "hu_pd": "https://github.com/Bayi-Hu/Pump-and-Dump-Detection-on-Cryptocurrency", "mirtaheri": "https://github.com/Mehrnoom/Cryptocurrency-Pump-Dump"}
BIG_GIT = ("hu_pd", "mirtaheri")
ZENODO = [("memechain/README.md", "18246856", "README.md"), ("victor_dex/EtherDollarPrice.csv", "4540223", "EtherDollarPrice.csv"),
          ("victor_dex/token_decimals.json", "4540223", "token_decimals.json"), ("memechain/data.zip", "18246856", "data.zip"),
          ("victor_dex/EtherDeltaTrades.csv", "4540223", "EtherDeltaTrades.csv"), ("victor_dex/IDEXTrades.csv", "4540223", "IDEXTrades.csv"),
          ("pumpsense/icbc_2026_reproducibility-v1.1.zip", "19616802", "AhmedMahrous00/icbc_2026_reproducibility-v1.1.zip")]


def main():
    ap = argparse.ArgumentParser(); ap.add_argument("--out", required=True); ap.add_argument("--skip-klines", action="store_true"); ap.add_argument("--workers", type=int, default=8); ap.add_argument("--kline-months", type=int, default=1, help="1 = pump month only, 3 = +-1 month")
    a = ap.parse_args()
    R = os.path.join(a.out, "crosspump"); os.makedirs(R, exist_ok=True)
    man = {"git": {}, "zenodo": {}, "klines": {}}
    steps = len(GIT) + len(ZENODO) + 1; k = 0
    prog = open(os.path.join(a.out, "progress.log"), "a")
    tick = lambda msg: (prog.write(f"{k}/{steps} {msg}\n"), prog.flush())
    def git_one(name, url):
        d = os.path.join(R, name)
        how = "existing"
        if not os.path.isdir(d) or not os.listdir(d):
            rc = subprocess.call(["git", "clone", "-q", "--depth", "1", url, d], stderr=subprocess.DEVNULL, timeout=3600) if not os.environ.get("XP_NO_GIT") else 1
            how = "git"
            if rc != 0:                                   # github.com blocked: fall back to the codeload archive of the default branch
                rc, how = codeload(url, d), "codeload"
        else:
            rc = 0
        commit = subprocess.run(["git", "-C", d, "rev-parse", "HEAD"], capture_output=True, text=True).stdout.strip() if how == "git" and rc == 0 else None
        man["git"][name] = {"url": url, "rc": rc, "via": how, "commit": commit}
        return rc
    for name, url in GIT.items():
        if name in BIG_GIT:
            continue
        rc = git_one(name, url); k += 1; tick(f"git {name} rc={rc}")
    if not a.skip_klines:
        man["klines"] = fetch_klines(R, a.workers, a.kline_months)
        json.dump(man, open(os.path.join(a.out, "fetch_manifest.json"), "w"), indent=1)
    k += 1; tick(f"klines {json.dumps({s_: sum(1 for v in man['klines'].values() if v == s_) for s_ in set(man['klines'].values())})}")
    for rel, rec, fname in ZENODO:
        dst = os.path.join(R, rel); os.makedirs(os.path.dirname(dst), exist_ok=True)
        url = f"https://zenodo.org/records/{rec}/files/{fname}?download=1"
        if not (os.path.exists(dst) and os.path.getsize(dst) > 0):
            rc = subprocess.call(["curl", "-sS", "-L", "--retry", "5", "-C", "-", "-o", dst, url])
        else:
            rc = 0
        man["zenodo"][rel] = {"record": rec, "rc": rc, "bytes": os.path.getsize(dst) if os.path.exists(dst) else 0}
        k += 1; tick(f"zenodo {rel} rc={rc}")
        json.dump(man, open(os.path.join(a.out, "fetch_manifest.json"), "w"), indent=1)
    for name in BIG_GIT:
        rc = git_one(name, GIT[name]); k += 1; tick(f"git {name} rc={rc}")
        json.dump(man, open(os.path.join(a.out, "fetch_manifest.json"), "w"), indent=1)
    if not a.skip_klines:                                        # hu_pd arrives late: top up klines for its pumps (existing files skipped)
        man["klines"] = fetch_klines(R, a.workers, a.kline_months)
    json.dump(man, open(os.path.join(a.out, "fetch_manifest.json"), "w"), indent=1)
    prog.write("done\n"); prog.close()


def codeload(url: str, d: str) -> int:
    """Download https://codeload.github.com/<owner>/<repo>/zip/HEAD and unpack it into d (top folder stripped)."""
    import shutil, tempfile
    owner_repo = url.rstrip("/").split("github.com/")[1]
    tmp = tempfile.mkdtemp(dir=os.path.dirname(d))
    z = os.path.join(tmp, "src.zip")
    for ref in ("HEAD", "refs/heads/master", "refs/heads/main"):
        rc = subprocess.call(["curl", "-sS", "-L", "--retry", "5", "-f", "-o", z, f"https://codeload.github.com/{owner_repo}/zip/{ref}"])
        if rc == 0:
            break
    if rc != 0:
        shutil.rmtree(tmp, ignore_errors=True); return rc
    with zipfile.ZipFile(z) as zf:
        zf.extractall(tmp)
    top = [x for x in os.listdir(tmp) if x != "src.zip"][0]
    if os.path.isdir(d):
        shutil.rmtree(d)
    shutil.move(os.path.join(tmp, top), d)
    shutil.rmtree(tmp, ignore_errors=True)
    return 0


def fetch_klines(R: str, workers: int, n_months: int = 1):
    """Binance 1-minute klines (data.binance.vision) for every Binance BTC-pair pump in the Hu, Sapienza and Ardia tables, +-1 month."""
    pairs = set()
    def months(dt):
        for d in ((0,) if n_months <= 1 else (-30, 0, 30)):
            m = dt + timedelta(days=d); yield f"{m.year:04d}-{m.month:02d}"
    hu = os.path.join(R, "hu_pd/Data/Telegram/Labeled/PD_logs_cleaned.txt")
    try:
        for r in (csv.DictReader(open(hu), delimiter="\t") if os.path.exists(hu) else []):
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
