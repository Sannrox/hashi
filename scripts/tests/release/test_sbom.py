import unittest
import uuid

from scripts.tests.release import load

sbom = load("sbom")

WORKSPACE = "hashi 0.0.0 (path+file:///workspace)"
METADATA = {
    "workspace_members": [WORKSPACE],
    "packages": [
        {"id": WORKSPACE, "name": "hashi", "version": "0.0.0", "license": "Apache-2.0"},
        {
            "id": "libc 0.2.0 (registry+https://github.com/rust-lang/crates.io-index)",
            "name": "libc",
            "version": "0.2.0",
            "license": "MIT/Apache-2.0",
        },
    ],
}


class BomFromMetadata(unittest.TestCase):
    def test_names_hashi_with_the_fields_attest_sbom_requires(self):
        bom = sbom.bom_from_metadata(METADATA)
        self.assertEqual(bom["bomFormat"], "CycloneDX")
        uuid.UUID(bom["serialNumber"].removeprefix("urn:uuid:"))
        self.assertEqual(bom["metadata"]["component"]["type"], "application")
        self.assertEqual(bom["metadata"]["component"]["purl"], "pkg:cargo/hashi@0.0.0")

    def test_lists_dependencies_with_spdx_licenses_but_not_the_workspace(self):
        [libc] = sbom.bom_from_metadata(METADATA)["components"]
        self.assertEqual(libc["name"], "libc")
        self.assertEqual(libc["licenses"], [{"expression": "MIT OR Apache-2.0"}])

    def test_serial_number_is_stable_for_the_same_contents(self):
        self.assertEqual(
            sbom.bom_from_metadata(METADATA)["serialNumber"],
            sbom.bom_from_metadata(METADATA)["serialNumber"],
        )

    def test_refuses_metadata_without_hashi(self):
        with self.assertRaisesRegex(SystemExit, "did not include package hashi"):
            sbom.bom_from_metadata({"workspace_members": [], "packages": []})


if __name__ == "__main__":
    unittest.main()
