#!/usr/bin/env python3
"""Registers the Kafka message schemas in Schema Registry, after checking compatibility.

This is the step that stops "one careless change breaks every consumer": the API never
registers schemas itself (auto.register.schemas=false), so the only way a new schema version
reaches the registry is through this script, and it refuses incompatible changes.

Usage:
    # Run by the schema-init container on `docker compose up`:
    python3 register_schemas.py

    # Before merging a .proto change (CI gate). Exits 2 if the change is incompatible:
    python3 infra/schema-registry/register_schemas.py --check-only

Connection settings come from flags or environment variables (SR_URL, SR_USER, SR_PASSWORD,
SR_CA_FILE). Only the Python standard library is used.
"""
from __future__ import annotations

import argparse
import base64
import json
import os
import ssl
import sys
import urllib.error
import urllib.request
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parent.parent

# Subject name = "<topic>-value" (Confluent's TopicNameStrategy) -> schema file.
SUBJECTS = {
    "order-events-value": "order_event.proto",
}


class RegistryError(Exception):
    pass


class Registry:
    def __init__(self, url: str, user: str, password: str, ca_file: str | None):
        self.url = url.rstrip("/")
        token = base64.b64encode(f"{user}:{password}".encode()).decode()
        self.headers = {
            "Authorization": f"Basic {token}",
            "Content-Type": "application/vnd.schemaregistry.v1+json",
            "Accept": "application/vnd.schemaregistry.v1+json",
        }
        self.context = None
        if self.url.startswith("https://"):
            if ca_file and not Path(ca_file).is_file():
                raise RegistryError(f"CA file {ca_file} not found. Start the stack once (it generates it) or pass --ca-file.")
            self.context = ssl.create_default_context(cafile=ca_file or None)

    def request(self, method: str, path: str, body: dict | None = None, allow_404: bool = False):
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(self.url + path, data=data, method=method, headers=self.headers)
        try:
            with urllib.request.urlopen(req, context=self.context, timeout=15) as resp:
                return json.loads(resp.read() or b"null")
        except urllib.error.HTTPError as e:
            if allow_404 and e.code == 404:
                return None
            detail = e.read().decode(errors="replace")
            raise RegistryError(f"{method} {path} -> HTTP {e.code}: {detail}") from None


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--url", default=os.environ.get("SR_URL", "https://localhost:8085"))
    parser.add_argument("--user", default=os.environ.get("SR_USER", "admin"))
    parser.add_argument("--password", default=os.environ.get("SR_PASSWORD"))
    parser.add_argument("--ca-file", default=os.environ.get("SR_CA_FILE", str(REPO / "infra/kafka/secrets/public/ca.crt")))
    parser.add_argument("--schemas-dir", default=os.environ.get("SCHEMAS_DIR", str(REPO / "src/BrewQueue.Contracts/Protos")))
    parser.add_argument("--compatibility", default=os.environ.get("SR_COMPATIBILITY", "BACKWARD_TRANSITIVE"))
    parser.add_argument("--check-only", action="store_true", help="only check compatibility; register nothing")
    args = parser.parse_args()

    if not args.password:
        print("Set SR_PASSWORD or pass --password.", file=sys.stderr)
        return 1

    registry = Registry(args.url, args.user, args.password, args.ca_file)
    failed = False

    for subject, file_name in SUBJECTS.items():
        schema_text = (Path(args.schemas_dir) / file_name).read_text(encoding="utf-8")
        body = {"schemaType": "PROTOBUF", "schema": schema_text}

        versions = registry.request("GET", f"/subjects/{subject}/versions", allow_404=True)
        if versions:
            result = registry.request(
                "POST", f"/compatibility/subjects/{subject}/versions/latest?verbose=true", body)
            if not result.get("is_compatible"):
                failed = True
                print(f"[INCOMPATIBLE] {subject}: {file_name} breaks compatibility with the registered schema:")
                for message in result.get("messages", []):
                    print(f"    - {message}")
                continue
            print(f"[ok] {subject}: compatible with version {max(versions)}")
        else:
            print(f"[new] {subject}: not registered yet")

        if args.check_only:
            continue

        registry.request("PUT", f"/config/{subject}", {"compatibility": args.compatibility})
        registered = registry.request("POST", f"/subjects/{subject}/versions?normalize=true", body)
        print(f"[registered] {subject}: schema id {registered['id']} (compatibility {args.compatibility})")

    return 2 if failed else 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except RegistryError as e:
        print(f"Schema Registry error: {e}", file=sys.stderr)
        sys.exit(1)
