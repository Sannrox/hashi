#!/usr/bin/env python3
"""Disposable foreign OPC UA server for Hashi interoperability tests.

Speaks OPC UA TCP with SignAndEncrypt, username users, raw Historical Access,
and an optional extra namespace used to shift the table across a restart.
"""
from __future__ import annotations

import asyncio
import os
import shutil
import signal
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from cryptography.x509.oid import ExtendedKeyUsageOID

from asyncua import Server, ua
from asyncua.crypto.cert_gen import setup_self_signed_certificate
from asyncua.crypto.truststore import TrustStore
from asyncua.crypto.validator import CertificateValidator, CertificateValidatorOptions
from asyncua.crypto.permission_rules import User, UserRole
from asyncua.server.history import HistoryDict
from asyncua.server.user_managers import UserManager


NS_URI = "urn:hashi:foreign"
EXTRA_NS_URI = "urn:hashi:foreign-extra"
START_MS = 1_700_000_000_000


class FixtureUsers(UserManager):
    def get_user(self, iserver, username=None, password=None, certificate=None):
        del iserver, certificate
        if username == "reader" and password == "fixture-password":
            return User(role=UserRole.Admin)
        return None


def utc_ms(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)


async def main() -> None:
    port = int(os.environ["HASHI_OPCUA_FOREIGN_PORT"])
    cert = Path(os.environ["HASHI_OPCUA_FOREIGN_CERT"])
    key = Path(os.environ["HASHI_OPCUA_FOREIGN_KEY"])
    client_cert = Path(os.environ["HASHI_OPCUA_FOREIGN_CLIENT_CERT"])
    client_key = Path(os.environ["HASHI_OPCUA_FOREIGN_CLIENT_KEY"])
    trust_dir = Path(os.environ["HASHI_OPCUA_FOREIGN_TRUST_DIR"])
    app_uri = os.environ.get("HASHI_OPCUA_FOREIGN_APP_URI", "urn:hashi:foreign-server")
    client_uri = os.environ.get(
        "HASHI_OPCUA_FOREIGN_CLIENT_URI", "urn:hashi:fixture-client"
    )
    extra_ns = os.environ.get("HASHI_OPCUA_FOREIGN_EXTRA_NS") == "1"
    page = int(os.environ.get("HASHI_OPCUA_FOREIGN_PAGE", "2"))
    subject = {
        "countryName": "DE",
        "stateOrProvinceName": "Test",
        "localityName": "Test",
        "organizationName": "Hashi",
    }
    uses = [ExtendedKeyUsageOID.SERVER_AUTH, ExtendedKeyUsageOID.CLIENT_AUTH]

    cert.parent.mkdir(parents=True, exist_ok=True)
    key.parent.mkdir(parents=True, exist_ok=True)
    client_cert.parent.mkdir(parents=True, exist_ok=True)
    client_key.parent.mkdir(parents=True, exist_ok=True)
    trust_dir.mkdir(parents=True, exist_ok=True)
    # Restart must present the same certificates Hashi already trusted.
    if not (cert.is_file() and key.is_file()):
        await setup_self_signed_certificate(
            key, cert, app_uri, "127.0.0.1", uses, subject
        )
    if not (client_cert.is_file() and client_key.is_file()):
        await setup_self_signed_certificate(
            client_key, client_cert, client_uri, "127.0.0.1", uses, subject
        )
    shutil.copy(client_cert, trust_dir / "client.der")

    server = Server(user_manager=FixtureUsers())
    await server.init()
    await server.set_application_uri(app_uri)
    endpoint = f"opc.tcp://127.0.0.1:{port}/"
    server.set_endpoint(endpoint)
    server.set_security_policy([ua.SecurityPolicyType.Basic256Sha256_SignAndEncrypt])
    server.set_identity_tokens([ua.UserNameIdentityToken])
    await server.load_certificate(str(cert))
    await server.load_private_key(str(key))

    trust_store = TrustStore([trust_dir], [])
    await trust_store.load()
    server.set_certificate_validator(
        CertificateValidator(
            options=CertificateValidatorOptions.TRUSTED_VALIDATION
            | CertificateValidatorOptions.PEER_CLIENT,
            trust_store=trust_store,
        )
    )

    storage = HistoryDict(max_history_data_response_size=page)
    await storage.init()
    server.iserver.history_manager.set_storage(storage)

    if extra_ns:
        await server.register_namespace(EXTRA_NS_URI)
    idx = await server.register_namespace(NS_URI)
    objects = server.nodes.objects
    equipment = await objects.add_object(ua.NodeId("Equipment", idx), "Equipment")
    temperature = await equipment.add_variable(
        ua.NodeId("Temperature", idx),
        "Temperature",
        21.5,
        varianttype=ua.VariantType.Double,
    )
    await temperature.set_read_only()
    eu = ua.EUInformation()
    eu.NamespaceUri = "urn:hashi:units"
    eu.UnitId = 1
    eu.DisplayName = ua.LocalizedText("C")
    eu.Description = ua.LocalizedText("temperature")
    await temperature.add_property(
        ua.NodeId("Units", idx),
        ua.QualifiedName("EngineeringUnits", 0),
        eu,
        datatype=ua.NodeId(ua.ObjectIds.EUInformation),
    )
    no_history = await equipment.add_variable(
        ua.NodeId("NoHistory", idx),
        "NoHistory",
        1.0,
        varianttype=ua.VariantType.Double,
    )
    await no_history.set_read_only()

    stop = asyncio.Event()

    def request_stop(*_args):
        stop.set()

    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)

    async def watch_stdin() -> None:
        await asyncio.to_thread(sys.stdin.read)
        stop.set()

    asyncio.create_task(watch_stdin())

    async with server:
        await server.iserver.enable_history_data_change(
            temperature, period=None, count=0
        )
        def sample_value(reading: float, ts: datetime) -> ua.DataValue:
            return ua.DataValue(
                ua.Variant(reading, ua.VariantType.Double),
                ua.StatusCode(ua.StatusCodes.Good),
                ts,
                ts,
            )

        for i in range(1, 21):
            ts = utc_ms(START_MS + i * 1000)
            await storage.save_node_value(
                temperature.nodeid, sample_value(float(i), ts)
            )
        # After the sequential range, so paging (START_MS, START_MS+5000] is
        # two distinct milliseconds and the same-ms window can fail closed.
        same_ms = START_MS + 25_000
        for i in range(5):
            ts = utc_ms(same_ms) + timedelta(microseconds=i * 100)
            await storage.save_node_value(
                temperature.nodeid, sample_value(100.0 + i, ts)
            )

        sys.stdout.write(f"READY {port}\n")
        sys.stdout.flush()
        sample = 21.5
        while not stop.is_set():
            try:
                await asyncio.wait_for(stop.wait(), timeout=0.2)
                break
            except TimeoutError:
                sample += 1.0
                await temperature.write_value(sample)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as error:
        sys.stderr.write(f"foreign OPC UA server failed: {error}\n")
        sys.exit(1)
