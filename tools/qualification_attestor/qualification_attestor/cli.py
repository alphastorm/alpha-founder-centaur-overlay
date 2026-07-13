from __future__ import annotations

import argparse
import json
import re
from collections.abc import Sequence

NONCE_PATTERN = re.compile(r"[A-Za-z0-9._:-]{1,128}\Z")


def attest(nonce: str) -> dict[str, object]:
    if NONCE_PATTERN.fullmatch(nonce) is None:
        raise ValueError("nonce must contain only A-Z, a-z, 0-9, dot, underscore, colon, or hyphen")
    return {
        "mutation": False,
        "network_used": False,
        "nonce": nonce,
        "result": "PASS",
        "tool": "qualification-attestor",
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Read-only Alpha Founder qualification attestor")
    subparsers = parser.add_subparsers(dest="command", required=True)
    attest_parser = subparsers.add_parser("attest", help="echo a bounded public nonce")
    attest_parser.add_argument("--nonce", required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.command != "attest":
        raise AssertionError(f"unhandled command: {args.command}")
    try:
        payload = attest(args.nonce)
    except ValueError as error:
        raise SystemExit(str(error)) from error
    print(json.dumps(payload, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
