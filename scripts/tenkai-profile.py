#!/usr/bin/env python3
"""Generate and check the Tenkai compatibility profile hashi publishes.

    scripts/tenkai-profile.py --check
    scripts/tenkai-profile.py --check --file PATH
    scripts/tenkai-profile.py --print-profile
    scripts/tenkai-profile.py --print-contract
    scripts/tenkai-profile.py --print-fields
    scripts/tenkai-profile.py --self-test

Contract ids: `connector.capture` (record/connector surface dependents
require) and `hashi.sink` (platform source-adapter protocol). Versions are
derived from those surfaces. `--check` refuses a product file that does not
end with the generated profile v2 block.
"""

from __future__ import annotations

import argparse
import hashlib
import pathlib
import re
import sys

CAPTURE_ID = "connector.capture"
SINK_ID = "hashi.sink"
PROFILE_VERSION = 2
MAX_EVIDENCE_AGE_MS = 86_400_000
COMPONENT = "hashi"
ROOT = pathlib.Path(__file__).resolve().parent.parent
CONTRACT_RS = ROOT / "src" / "contract.rs"
PLATFORM_RS = ROOT / "src" / "platform.rs"
CARGO = ROOT / "Cargo.toml"
PRODUCT = ROOT / "deploy" / "tenkai.toml"
SEMVER = re.compile(r"(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)")

# Semantic versions of the capture surface. Adding an optional field is a
# minor; removing, renaming, or changing meaning is a major. The integer
# CONTRACT_VERSION is part of the key so a persistence bump cannot stay silent.
CAPTURE_VERSIONS = {
    (
        2,
        ("source", "kind", "key", "source_ts_ms", "ingest_ts_ms", "quality", "position", "payload"),
        ("cursor", "change"),
        ("operation", "before_image", "transaction_id", "commit_ts_ms"),
        ("Insert", "Update", "Delete", "TransactionBegin", "TransactionEnd"),
        ("ok", "message"),
        ("kind", "schema", "metadata"),
        ("connector", "instance", "record_kind", "key", "position"),
    ): "0.2.0",
}

# Semantic versions of the platform sink protocol. The protocol name, frame
# version, and magic identify the surface dependents call.
SINK_VERSIONS = {
    ("hashi.sink.v2", 2, "HSK1", "hashi.sink.v1"): "0.2.0",
}


def fail(message: str) -> None:
    raise SystemExit(f"tenkai-profile: {message}")


def rust_struct_fields(source: str, name: str) -> tuple[str, ...]:
    marker = f"struct {name} {{"
    start = source.find(marker)
    if start < 0:
        fail(f"missing struct {name}")
    i = start + len(marker)
    depth = 1
    while i < len(source) and depth:
        char = source[i]
        if char == "{":
            depth += 1
        elif char == "}":
            depth -= 1
        i += 1
    body = source[start + len(marker) : i - 1]
    fields: list[str] = []
    for raw in body.splitlines():
        line = raw.strip()
        if not line or line.startswith("//") or line.startswith("#["):
            continue
        ident, sep, _rest = line.partition(":")
        if sep != ":":
            continue
        ident = ident.strip().removeprefix("pub ").strip()
        if ident.isidentifier():
            fields.append(ident)
    if not fields:
        fail(f"struct {name} has no fields")
    return tuple(fields)


def rust_enum_variants(source: str, name: str) -> tuple[str, ...]:
    marker = f"enum {name} {{"
    start = source.find(marker)
    if start < 0:
        fail(f"missing enum {name}")
    i = start + len(marker)
    depth = 1
    while i < len(source) and depth:
        char = source[i]
        if char == "{":
            depth += 1
        elif char == "}":
            depth -= 1
        i += 1
    body = source[start + len(marker) : i - 1]
    variants: list[str] = []
    for raw in body.splitlines():
        line = raw.strip().rstrip(",")
        if not line or line.startswith("//") or line.startswith("#["):
            continue
        if line.isidentifier():
            variants.append(line)
    if not variants:
        fail(f"enum {name} has no variants")
    return tuple(variants)


def rust_const_u32(source: str, name: str) -> int:
    match = re.search(
        rf"const {re.escape(name)}: u32 = ([0-9]+);",
        source,
    )
    if not match:
        fail(f"missing const {name}: u32")
    return int(match.group(1))


def rust_const_str(source: str, name: str) -> str:
    match = re.search(
        rf'const {re.escape(name)}: &str = "([^"]+)";',
        source,
    )
    if not match:
        fail(f"missing const {name}: &str")
    return match.group(1)


def capture_surface(source: str | None = None) -> tuple:
    text = CONTRACT_RS.read_text() if source is None else source
    return (
        rust_const_u32(text, "CONTRACT_VERSION"),
        rust_struct_fields(text, "Record"),
        rust_struct_fields(text, "SourceMetadata"),
        rust_struct_fields(text, "Change"),
        rust_enum_variants(text, "Operation"),
        rust_struct_fields(text, "CheckReport"),
        rust_struct_fields(text, "DiscoveredKind"),
        rust_struct_fields(text, "IdempotencyKey"),
    )


def sink_surface(source: str | None = None) -> tuple:
    text = PLATFORM_RS.read_text() if source is None else source
    magic = re.search(r'const MAGIC: &\[u8; 4\] = b"([A-Z0-9]+)";', text)
    if not magic:
        fail("missing MAGIC on the platform sink protocol")
    return (
        rust_const_str(text, "PLATFORM_SINK_PROTOCOL"),
        rust_const_u32(text, "VERSION"),
        magic.group(1),
        rust_const_str(text, "LEGACY_PROTOCOL"),
    )


def lookup_version(table: dict, key: tuple, label: str) -> str:
    try:
        version = table[key]
    except KeyError:
        fail(f"unknown {label} surface {key!r}; record a contract version")
    if not SEMVER.fullmatch(version):
        fail(f"{label} contract version must be stable MAJOR.MINOR.PATCH: {version}")
    return version


def capture_version(surface: tuple | None = None) -> str:
    return lookup_version(
        CAPTURE_VERSIONS,
        capture_surface() if surface is None else surface,
        "capture",
    )


def sink_version(surface: tuple | None = None) -> str:
    return lookup_version(
        SINK_VERSIONS,
        sink_surface() if surface is None else surface,
        "sink",
    )


def crate_version() -> str:
    text = CARGO.read_text()
    match = re.search(
        r'(?ms)^\[package\][^\[]*?^version\s*=\s*"([^"]+)"',
        text,
    )
    if not match:
        fail("Cargo.toml has no [package] version")
    return match.group(1)


def product_version(body: str) -> str:
    match = re.search(
        r'(?ms)^\[product\].*?^version = "([^"]+)"',
        body,
    )
    if not match:
        fail("product file is missing [product].version")
    return match.group(1)


def pin_digest(version: str) -> str:
    # Version-bound pin so the committed product can be checked without a
    # built image. The family release manifest carries the image digest.
    return "sha256:" + hashlib.sha256(f"hashi@{version}".encode()).hexdigest()


def compatibility_profile(
    product_ver: str,
    capture_ver: str,
    sink_ver: str,
) -> str:
    pin = pin_digest(product_ver)
    return (
        "[compatibility]\n"
        f"version = {PROFILE_VERSION}\n"
        "# Evidence binds one release digest; one day is Tenkai's maximum.\n"
        f"max_evidence_age_ms = {MAX_EVIDENCE_AGE_MS}\n"
        "\n"
        f"[compatibility.components.{COMPONENT}.pin]\n"
        'kind = "digest"\n'
        f'value = "{pin}"\n'
        "\n"
        f"[compatibility.components.{COMPONENT}.provides]\n"
        f'"{CAPTURE_ID}" = "{capture_ver}"\n'
        f'"{SINK_ID}" = "{sink_ver}"\n'
    )


def contracts() -> tuple[str, str, str, str]:
    return CAPTURE_ID, capture_version(), SINK_ID, sink_version()


def check_tenkai_toml(
    body: str,
    contract_src: str | None = None,
    platform_src: str | None = None,
) -> None:
    product_ver = product_version(body)
    crate_ver = crate_version()
    if product_ver != crate_ver:
        fail(f"product.version {product_ver} must match crate version {crate_ver}")
    if ":latest" in body or "/latest" in body:
        fail("product file must not use a latest tag")
    image = f"ghcr.io/sannrox/hashi:{product_ver}"
    if image not in body:
        fail(f"product file must install {image}")
    capture_ver = capture_version(capture_surface(contract_src) if contract_src is not None else None)
    sink_ver = sink_version(sink_surface(platform_src) if platform_src is not None else None)
    profile = compatibility_profile(product_ver, capture_ver, sink_ver)
    if not body.endswith(profile):
        fail("deploy/tenkai.toml must end with this compatibility profile:\n\n" + profile)


def generated_profile() -> str:
    body = PRODUCT.read_text()
    capture_id, capture_ver, sink_id, sink_ver = contracts()
    del capture_id, sink_id
    return compatibility_profile(product_version(body), capture_ver, sink_ver)


def self_test() -> None:
    contract_src = CONTRACT_RS.read_text()
    platform_src = PLATFORM_RS.read_text()
    capture = capture_surface(contract_src)
    sink = sink_surface(platform_src)
    capture_ver = capture_version(capture)
    sink_ver = sink_version(sink)
    body = PRODUCT.read_text()
    check_tenkai_toml(body, contract_src, platform_src)
    profile = compatibility_profile(product_version(body), capture_ver, sink_ver)
    if f"version = {PROFILE_VERSION}\n" not in profile:
        fail("self-test: generated profile is not v2")
    if f'"{CAPTURE_ID}" = "{capture_ver}"' not in profile:
        fail("self-test: generated profile missing the capture contract")
    if f'"{SINK_ID}" = "{sink_ver}"' not in profile:
        fail("self-test: generated profile missing the sink contract")

    extra_record = (
        capture[0],
        capture[1] + ("extra",),
        *capture[2:],
    )
    try:
        capture_version(extra_record)
    except SystemExit:
        pass
    else:
        fail("self-test: extra capture field must be unknown")

    extra_sink = (sink[0], sink[1] + 1, sink[2], sink[3])
    try:
        sink_version(extra_sink)
    except SystemExit:
        pass
    else:
        fail("self-test: unknown sink protocol version must be refused")

    mutated = [
        body.replace(f'"{CAPTURE_ID}" = "{capture_ver}"', f'"{CAPTURE_ID}" = "0.0.1"'),
        body.replace(f'"{SINK_ID}" = "{sink_ver}"', f'"{SINK_ID}" = "0.0.1"'),
        body.replace(f'"{CAPTURE_ID}"', '"hashi.capture"'),
        body.replace("version = 2", "version = 1", 1),
    ]
    for stale in mutated:
        if stale == body:
            fail("self-test: mutation did not change the product")
        try:
            check_tenkai_toml(stale, contract_src, platform_src)
        except SystemExit:
            continue
        fail("self-test: stale product must be refused")


def main(argv: list[str]) -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--check", action="store_true")
    mode.add_argument("--print-profile", action="store_true")
    mode.add_argument("--print-contract", action="store_true")
    mode.add_argument("--print-fields", action="store_true")
    mode.add_argument("--self-test", action="store_true")
    parser.add_argument("--file", type=pathlib.Path)
    args = parser.parse_args(argv)
    if args.self_test:
        if args.file:
            parser.error("--self-test does not take --file")
        self_test()
        _, capture_ver, _, sink_ver = contracts()
        print(f"tenkai-profile: {CAPTURE_ID} {capture_ver} {SINK_ID} {sink_ver} ok")
        return
    if args.print_fields:
        capture = capture_surface()
        sink = sink_surface()
        print("capture")
        print(f"contract_version {capture[0]}")
        print("record " + " ".join(capture[1]))
        print("source_metadata " + " ".join(capture[2]))
        print("change " + " ".join(capture[3]))
        print("operation " + " ".join(capture[4]))
        print("check_report " + " ".join(capture[5]))
        print("discovered_kind " + " ".join(capture[6]))
        print("idempotency_key " + " ".join(capture[7]))
        print("---")
        print(
            f"protocol {sink[0]} version {sink[1]} magic {sink[2]} legacy {sink[3]}"
        )
        return
    if args.print_contract:
        _, capture_ver, _, sink_ver = contracts()
        print(f"{CAPTURE_ID} {capture_ver}")
        print(f"{SINK_ID} {sink_ver}")
        return
    if args.print_profile:
        sys.stdout.write(generated_profile())
        return
    path = args.file if args.file is not None else PRODUCT
    check_tenkai_toml(path.read_text())
    _, capture_ver, _, sink_ver = contracts()
    print(f"tenkai-profile: {CAPTURE_ID} {capture_ver} {SINK_ID} {sink_ver} ok")


if __name__ == "__main__":
    main(sys.argv[1:])
