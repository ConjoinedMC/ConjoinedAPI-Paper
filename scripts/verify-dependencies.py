"""Check resolved JAR bytes and emit the complete pinned advisory inventory."""
from pathlib import Path
import hashlib
import json
import re

components = []
for line in Path("dependencies.lock").read_text().splitlines():
    digest, group, name, version, filename = line.split()
    jar = Path("target/locked-dependencies") / filename
    if hashlib.sha256(jar.read_bytes()).hexdigest() != digest:
        raise SystemExit(f"Dependency checksum mismatch: {filename}")
    components.append({"type": "library", "group": group, "name": name,
                       "version": version, "purl": f"pkg:maven/{group}/{name}@{version}"})

root = json.loads(Path("target/dependency-tree.json").read_text())
resolved = set()
def coordinate(group, name, version):
    # copy-dependencies may canonicalize a pinned timestamp to its SNAPSHOT base.
    # JAR hashes still enforce the exact bytes; POMs pin the retrievable timestamp.
    version = re.sub(r"-\d{8}\.\d{6}-\d+$", "-SNAPSHOT", version)
    return group, name, version
def visit(node):
    if node is not root:
        resolved.add(coordinate(node["groupId"], node["artifactId"], node["version"]))
    for child in node.get("children", []):
        visit(child)
visit(root)
locked = {coordinate(item["group"], item["name"], item["version"]) for item in components}
if resolved != locked:
    raise SystemExit("Resolved dependency graph differs from dependencies.lock")

Path("target/dependencies.cdx.json").write_text(json.dumps({
    "bomFormat": "CycloneDX", "specVersion": "1.5", "version": 1, "components": components}))
print(f"Verified {len(components)} dependency checksums")
