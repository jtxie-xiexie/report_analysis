#!/usr/bin/env python3
"""
Smoke test DolphinDB connectivity and base execution.
"""

from __future__ import annotations

import argparse

import dolphindb as ddb


def main() -> int:
    parser = argparse.ArgumentParser(description="Smoke test DolphinDB connection")
    parser.add_argument("--host", required=True)
    parser.add_argument("--port", required=True, type=int)
    parser.add_argument("--user", required=True)
    parser.add_argument("--password", required=True)
    parser.add_argument("--ssl", action="store_true")
    args = parser.parse_args()

    session = ddb.session(enableSSL=args.ssl)
    session.connect(args.host, args.port, args.user, args.password)
    value = session.run("1+1")
    print(f"connected=true result={value}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
