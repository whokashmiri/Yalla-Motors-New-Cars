from __future__ import annotations
import datetime, sys

def log(*args):
    ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    msg = " ".join(str(a) for a in args)
    print(f"[{ts}] {msg}", file=sys.stdout, flush=True)
