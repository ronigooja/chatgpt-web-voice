#!/usr/bin/env python3
"""Regression tests for the Python embedded in deploy-host.sh."""

import pathlib
import subprocess
import sys
import tempfile
import unittest

SCRIPT = pathlib.Path(__file__).with_name("deploy-host.sh").read_text()
UPDATE = SCRIPT.split("<<'PY'\n", 1)[1].split("\nPY\n", 1)[0]
HOST = '''services:
  voice:
    image: "old:tag"
    network_mode: host
    environment:
      VOICE_RELAY_ENABLED: "true"
      VOICE_RELAY_PUBLIC_IP: "192.0.2.1"
      VOICE_RELAY_UDP_PORT_MIN: "40000"
      VOICE_RELAY_UDP_PORT_MAX: "40099"
    secrets:
      - token

secrets:
  token:
    file: .secrets/token
volumes:
  data:
'''
BRIDGE = HOST.replace('    network_mode: host\n', '')


def update(path, dry_run="false"):
    return subprocess.run(
        [sys.executable, "-", str(path), "new:tag", "192.0.2.1", "40000", "40099", dry_run],
        input=UPDATE, text=True, capture_output=True, check=True,
    )


class DeployHostTest(unittest.TestCase):
    def test_host_and_bridge(self):
        for source, host in ((HOST, True), (BRIDGE, False)):
            with self.subTest(host=host), tempfile.TemporaryDirectory() as directory:
                path = pathlib.Path(directory) / "compose.yaml"
                path.write_text(source)
                update(path, "true")
                self.assertEqual(path.read_text(), source)
                update(path)
                actual = path.read_text()
                expected = source.replace('image: "old:tag"', 'image: "new:tag"')
                if not host:
                    expected = expected.replace('\n\nsecrets:', '\n    ports:\n      - "40000-40099:40000-40099/udp"\n\nsecrets:')
                self.assertEqual(actual, expected)
                # PyYAML is optional; exact fixture comparison above also checks structure.
                try:
                    import yaml
                except ImportError:
                    pass
                else:
                    parsed = yaml.safe_load(actual)
                    self.assertEqual("ports" in parsed["services"]["voice"], not host)
                    self.assertIn("token", parsed["secrets"])
                    self.assertIn("data", parsed["volumes"])
                update(path)
                self.assertEqual(path.read_text(), actual)

    def test_existing_ports_and_sibling(self):
        source = BRIDGE.replace('    secrets:\n', '    ports:\n      - "127.0.0.1:8080:80"\n    secrets:\n', 1)
        source = source.replace('\nsecrets:\n', '\n  other:\n    image: other:tag\nsecrets:\n', 1)
        with tempfile.TemporaryDirectory() as directory:
            path = pathlib.Path(directory) / "compose.yaml"
            path.write_text(source)
            update(path)
            actual = path.read_text()
            self.assertIn('      - "127.0.0.1:8080:80"\n      - "40000-40099:40000-40099/udp"\n    secrets:', actual)
            update(path)
            self.assertEqual(path.read_text(), actual)


if __name__ == "__main__":
    unittest.main()
