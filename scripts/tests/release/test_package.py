"""package.sh dry runs against this checkout."""

import json
import os
import pathlib
import shutil
import subprocess
import tempfile
import tomllib
import unittest

from scripts.tests.release import ROOT, RELEASE, load

SCRIPT = RELEASE / "package.sh"
notes = load("notes")


class Package(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.sha = subprocess.run(
            ["git", "-C", str(ROOT), "rev-parse", "HEAD"], check=True, capture_output=True, text=True
        ).stdout.strip()
        cls.version = tomllib.loads((ROOT / "Cargo.toml").read_text())["package"]["version"]

    def setUp(self):
        tmp = tempfile.TemporaryDirectory(prefix=f"pkg{os.getpid()}-")
        self.addCleanup(tmp.cleanup)
        self.tmp = pathlib.Path(tmp.name)
        self.dist = self.tmp / "dist"

    def package(self, *args: str, path: str | None = None) -> subprocess.CompletedProcess:
        env = {**os.environ, "PATH": path or os.environ["PATH"]}
        return subprocess.run(
            ["bash", str(SCRIPT), "--out", str(self.dist), "--skip-build", "--skip-test", "--git-sha", self.sha, *args],
            env=env, capture_output=True, text=True,
        )

    def fake_gh(self, body: str) -> str:
        """PATH with a gh that runs `body`, ahead of the real one."""
        bin_dir = pathlib.Path(tempfile.mkdtemp(dir=self.tmp))
        (bin_dir / "gh").write_text(f"#!/usr/bin/env bash\n{body}\n")
        (bin_dir / "gh").chmod(0o755)
        return f"{bin_dir}:{os.environ['PATH']}"

    def test_dry_run_writes_manifest_sbom_and_checksums_without_latest(self):
        result = self.package("--skip-github")
        self.assertEqual(result.returncode, 0, result.stderr)
        manifest = json.loads((self.dist / "hashi-release.json").read_text())
        self.assertEqual(manifest["schema"], "tenkai.family-release-manifest.v1")
        self.assertEqual(manifest["component"], "hashi")
        self.assertEqual(manifest["git_sha"], self.sha)
        self.assertEqual(manifest["images"], [])
        self.assertNotIn("latest", json.dumps(manifest))
        self.assertTrue((self.dist / "hashi.cdx.json").is_file())
        self.assertTrue((self.dist / "SHA256SUMS").is_file())

    def test_tagged_run_writes_notes_from_changelog(self):
        result = self.package("--skip-github", "--tag", f"v{self.version}")
        self.assertEqual(result.returncode, 0, result.stderr)
        expected = notes.notes_for((ROOT / "CHANGELOG.md").read_text(), self.version)
        self.assertEqual((self.dist / "RELEASE_NOTES.md").read_text().strip(), expected)

    def test_tag_must_match_the_crate_version(self):
        result = self.package("--skip-github", "--tag", "v999.0.0")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("does not match crate version", result.stderr)

    def test_tagged_run_reads_gate_flags_when_the_lookup_succeeds(self):
        result = self.package("--tag", f"v{self.version}", path=self.fake_gh('echo "[]"'))
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(json.loads((self.dist / "hashi-release.json").read_text())["gate_flags"], [])

    def test_tagged_run_fails_closed_when_gate_flags_cannot_be_read(self):
        # Missing or truncated gate flags would read as a release that passed no gates.
        no_gh = self.tmp / "no-gh"
        no_gh.mkdir()
        for tool in ("bash", "dirname"):
            (no_gh / tool).symlink_to(shutil.which(tool))
        at_limit = """python3 -c 'import json; print(json.dumps([{"mergeCommit": None, "labels": []}] * 1000))'"""
        for message, path in (
            ("gh is required", str(no_gh)),
            ("could not read merged pull requests", self.fake_gh("echo 'HTTP 401: Bad credentials' >&2; exit 1")),
            ("reached the 1000 pull request limit", self.fake_gh(at_limit)),
        ):
            with self.subTest(message=message):
                shutil.rmtree(self.dist, ignore_errors=True)
                result = self.package("--tag", f"v{self.version}", path=path)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn(message, result.stderr)
                self.assertFalse((self.dist / "hashi-release.json").exists())


if __name__ == "__main__":
    unittest.main()
