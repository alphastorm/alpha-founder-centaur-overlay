from __future__ import annotations

import importlib.util
import io
import json
from contextlib import redirect_stdout
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parents[1]
CLI = ROOT / "tools/qualification_attestor/qualification_attestor/cli.py"
SPEC = importlib.util.spec_from_file_location("qualification_attestor_cli", CLI)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class QualificationAttestorTests(unittest.TestCase):
    def test_attestation_is_exact_and_non_mutating(self) -> None:
        self.assertEqual(
            MODULE.attest("run-a:q04"),
            {
                "mutation": False,
                "network_used": False,
                "nonce": "run-a:q04",
                "result": "PASS",
                "tool": "qualification-attestor",
            },
        )

    def test_rejects_unbounded_nonce(self) -> None:
        with self.assertRaisesRegex(ValueError, "nonce must contain"):
            MODULE.attest("secret shaped input")

    def test_cli_emits_machine_readable_json(self) -> None:
        output = io.StringIO()
        with redirect_stdout(output):
            result = MODULE.main(["attest", "--nonce", "run-b:q11"])
        self.assertEqual(result, 0)
        self.assertEqual(json.loads(output.getvalue())["nonce"], "run-b:q11")


if __name__ == "__main__":
    unittest.main()
