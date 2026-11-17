import json
import pathlib
import tempfile
import unittest

from scripts.tests.release import load

manifest = load("manifest")


def pr(oid: str | None, *labels: str) -> dict:
    return {"mergeCommit": {"oid": oid} if oid else None, "labels": [{"name": n} for n in labels]}


class GateFlags(unittest.TestCase):
    def test_maps_gate_labels_and_ignores_others(self):
        flags = manifest.gate_flags_from_labels(["risk:breaking", "area:ops", "GATE:mig", "risk:realm", "docs"])
        self.assertEqual(flags, ["breaking", "migration", "realm"])

    def test_only_pull_requests_merged_into_the_release_range_count(self):
        prs = [
            pr("aaa", "risk:breaking", "docs"),  # in range
            pr("bbb", "gate:mig"),  # merged after the release commit
            pr("ccc", "risk:realm"),  # before the previous final tag
            pr(None, "risk:realm"),  # no merge commit
        ]
        self.assertEqual(manifest.labels_in_range(prs, "aaa\nddd\n", 1000), ["docs", "risk:breaking"])

    def test_a_lookup_at_the_limit_fails_closed(self):
        # A truncated list could drop a gate flag.
        with self.assertRaisesRegex(SystemExit, "reached the 2 pull request limit"):
            manifest.labels_in_range([pr("aaa"), pr("bbb")], "aaa\n", 2)


class BuildManifest(unittest.TestCase):
    def build(self, dist: pathlib.Path, **overrides) -> dict:
        args = dict(
            component="hashi", version="0.1.0", git_sha="a" * 40, tag="v0.1.0",
            dist=dist, images=[], attestations=[], gate_flags=[],
        )
        return manifest.build_manifest(**{**args, **overrides})

    def test_refuses_a_short_git_sha(self):
        with self.assertRaisesRegex(SystemExit, "full lowercase SHA"):
            self.build(pathlib.Path("."), git_sha="deadbeef")

    def test_lists_only_release_binaries_by_digest(self):
        with tempfile.TemporaryDirectory() as tmp:
            dist = pathlib.Path(tmp)
            (dist / "hashi-0.1.0-x86_64-unknown-linux-gnu").write_bytes(b"bin")
            (dist / "hashi-0.1.0-image.tar").write_bytes(b"tar")
            for name in ("SHA256SUMS", "hashi-release.json", "hashi.cdx.json", "RELEASE_NOTES.md"):
                (dist / name).write_text("{}\n")
            doc = self.build(dist)
        self.assertEqual(doc["schema"], manifest.SCHEMA)
        self.assertEqual([b["name"] for b in doc["binaries"]], ["hashi-0.1.0-x86_64-unknown-linux-gnu"])
        self.assertTrue(doc["binaries"][0]["digest"].startswith("sha256:"))

    def test_images_are_named_by_digest_and_never_latest(self):
        image = manifest.parse_image("hashi:v0.1.0=sha256:" + "b" * 64)
        self.assertEqual(image, {"name": "hashi:v0.1.0", "digest": "sha256:" + "b" * 64})
        with self.assertRaisesRegex(SystemExit, "latest"):
            manifest.parse_image("hashi:latest=sha256:" + "b" * 64)

    def test_record_attestations_drops_empty_urls(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = pathlib.Path(tmp) / "m.json"
            path.write_text(json.dumps({"attestations": [], "component": "hashi"}))
            manifest.main(["record-attestations", str(path), "https://example.invalid/1", ""])
            doc = json.loads(path.read_text())
        self.assertEqual(doc["attestations"], ["https://example.invalid/1"])
        self.assertEqual(doc["component"], "hashi")


if __name__ == "__main__":
    unittest.main()
