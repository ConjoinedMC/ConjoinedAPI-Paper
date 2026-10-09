"""Check resolved JAR bytes and emit the complete pinned advisory inventory."""
from pathlib import Path
import hashlib
import json

components = []
for line in Path("dependencies.lock").read_text().splitlines():
    digest, group, name, version, filename = line.split()
    jar = Path("target/locked-dependencies") / filename
    if hashlib.sha256(jar.read_bytes()).hexdigest() != digest:
        raise SystemExit(f"Dependency checksum mismatch: {filename}")
    components.append({"type": "library", "group": group, "name": name,
                       "version": version, "purl": f"pkg:maven/{group}/{name}@{version}"})
Path("target/dependencies.cdx.json").write_text(json.dumps({
    "bomFormat": "CycloneDX", "specVersion": "1.5", "version": 1, "components": components}))
print(f"Verified {len(components)} dependency checksums")
