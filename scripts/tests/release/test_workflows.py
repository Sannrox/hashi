"""Release invariants no workflow linter expresses, asserted on the parsed YAML.

The scripts enforce these at run time; these tests keep the workflows from
bypassing them. Assert on structure (triggers, permissions, job graph, step
order, which script a step runs), never on the wording of a step.
"""

import unittest

import yaml

from scripts.tests.release import ROOT

WORKFLOWS = ROOT / ".github" / "workflows"
VERIFIED_SHA = "${{ needs.verify-tag.outputs.sha }}"
TAGGED = "needs.verify-tag.outputs.tag != ''"


def load(name: str) -> dict:
    doc = yaml.safe_load((WORKFLOWS / name).read_text())
    doc["on"] = doc.pop(True, doc.get("on"))  # YAML 1.1 reads a bare `on` key as True.
    return doc


def runs(step: dict) -> str:
    return step.get("run", "")


def uses(step: dict) -> str:
    return step.get("uses", "")


def is_mutation(step: dict) -> bool:
    return (
        uses(step).startswith("actions/attest")
        or "docker push" in runs(step)
        or "github-release.sh draft" in runs(step)
        or "github-release.sh publish" in runs(step)
    )


def is_guard(step: dict) -> bool:
    return "github-release.sh guard" in runs(step)


def needs(job: dict) -> set:
    value = job.get("needs", [])
    return {value} if isinstance(value, str) else set(value)


def index(steps: list, predicate) -> int:
    return next(i for i, s in enumerate(steps) if predicate(s))


def strings(node):
    if isinstance(node, dict):
        for value in node.values():
            yield from strings(value)
    elif isinstance(node, list):
        for value in node:
            yield from strings(value)
    elif isinstance(node, str):
        yield node


class Dispatcher(unittest.TestCase):
    """A v* tag push only starts release.yml from main; it can do nothing else."""

    def setUp(self):
        self.wf = load("release-dispatch.yml")

    def test_triggers_only_on_v_tags(self):
        self.assertEqual(self.wf["on"], {"push": {"tags": ["v*"]}})

    def test_can_only_dispatch_workflows(self):
        self.assertEqual(self.wf["permissions"], {"actions": "write", "contents": "read"})

    def test_runs_one_step_that_dispatches_release_on_main(self):
        [job] = self.wf["jobs"].values()
        [step] = job["steps"]
        self.assertNotIn("uses", step)
        self.assertEqual(step["run"].strip(), 'gh workflow run release.yml --ref main -f tag="$TAG"')


class Release(unittest.TestCase):
    def setUp(self):
        self.wf = load("release.yml")
        self.jobs = self.wf["jobs"]

    def test_never_runs_on_push(self):
        self.assertNotIn("push", self.wf["on"])
        self.assertIn("tag", self.wf["on"]["workflow_dispatch"]["inputs"])

    def test_one_run_per_tag(self):
        self.assertIn("inputs.tag", self.wf["concurrency"]["group"])
        self.assertFalse(self.wf["concurrency"]["cancel-in-progress"])

    def test_tagged_runs_start_from_main(self):
        first = self.jobs["verify-tag"]["steps"][0]
        self.assertEqual(first["if"], "inputs.tag != ''")
        self.assertEqual(first["env"]["WORKFLOW_REF"], "${{ github.ref }}")
        self.assertIn("refs/heads/main", first["run"])

    def test_verifies_the_tag_with_mains_verifier_and_keys(self):
        steps = self.jobs["verify-tag"]["steps"]
        main_checkout = next(s for s in steps if s.get("with", {}).get("path") == "main-tree")
        self.assertNotIn("ref", main_checkout["with"], "main's verifier must come from the workflow ref")
        verify = next(s for s in steps if "verify-tag.sh" in runs(s))
        self.assertTrue(verify["env"]["MAIN_TREE"].endswith("/main-tree"))
        self.assertIn('bash "${MAIN_TREE}/scripts/release/verify-tag.sh" verify', verify["run"])
        self.assertIn("${MAIN_TREE}/security/release-tag-fingerprints", verify["run"])
        self.assertIn("merge-base --is-ancestor", verify["run"], "the tagged commit must be on main")

    def test_every_later_job_builds_the_verified_commit(self):
        for name, job in self.jobs.items():
            if name == "verify-tag":
                continue
            with self.subTest(job=name):
                self.assertIn("verify-tag", needs(job))
                steps = job["steps"]
                for i, step in enumerate(steps):
                    if uses(step).startswith("actions/checkout@"):
                        self.assertEqual(step.get("with", {}).get("ref"), VERIFIED_SHA)
                        self.assertIn('test "$(git rev-parse HEAD)" = "$SHA"', runs(steps[i + 1]))

    def test_never_uses_the_event_commit_or_ref_name(self):
        for value in strings(self.jobs):
            for banned in ("github.sha", "github.ref_name", "GITHUB_SHA", "GITHUB_REF", "github.event_name"):
                self.assertNotIn(banned, value)

    def test_release_changes_go_through_the_release_script(self):
        for value in strings(self.jobs):
            self.assertNotIn("gh release ", value)

    def test_every_mutating_job_refuses_a_published_release_first(self):
        mutating = set()
        for name, job in self.jobs.items():
            steps = job["steps"]
            first = next((i for i, s in enumerate(steps) if is_mutation(s)), None)
            if first is None:
                continue
            mutating.add(name)
            with self.subTest(job=name):
                guards = [s for s in steps[:first] if is_guard(s)]
                self.assertTrue(guards, f"{name} mutates before the guard")
                self.assertTrue(all("if" not in s for s in guards), f"{name} guard must always run")
                self.assertEqual(job.get("if"), TAGGED)
        self.assertEqual(mutating, {"push-image", "attest", "publish"})

    def test_release_tests_gate_packaging(self):
        self.assertIn("tests", needs(self.jobs["package"]))

    def test_publish_runs_last_after_every_artifact_job(self):
        publish = self.jobs["publish"]
        self.assertLessEqual({"package", "attest", "push-image"}, needs(publish))
        self.assertFalse(any("publish" in needs(job) for job in self.jobs.values()))

    def test_release_notes_come_from_changelog(self):
        steps = self.jobs["publish"]["steps"]
        notes = index(steps, lambda s: "scripts/release/notes.py" in runs(s))
        draft = index(steps, lambda s: "github-release.sh draft" in runs(s))
        self.assertLess(notes, draft)

    def test_packaging_can_read_merged_pull_requests_for_gate_flags(self):
        packagers = [job for job in self.jobs.values() if any("release/package.sh" in runs(s) for s in job["steps"])]
        self.assertTrue(packagers)
        for job in packagers:
            self.assertEqual(job["permissions"].get("pull-requests"), "read")

    def test_write_permissions_are_job_scoped(self):
        self.assertEqual(self.wf["permissions"], {"contents": "read"})
        writers = {
            name: {k for k, v in job.get("permissions", {}).items() if v == "write"}
            for name, job in self.jobs.items()
        }
        self.assertEqual(
            {name: perms for name, perms in writers.items() if perms},
            {"push-image": {"packages"}, "attest": {"id-token", "attestations"}, "publish": {"contents"}},
        )

    def test_never_produces_a_latest_image_tag(self):
        for value in strings(self.jobs):
            self.assertNotIn(":latest", value)


if __name__ == "__main__":
    unittest.main()
