"""Load the selected Core schema checkout and record its provenance."""

from __future__ import annotations

import hashlib
from pathlib import Path
import shutil
import subprocess
from typing import Any

from .errors import IPCError, UnsupportedInterface

# This module lives one directory below the project root.
DEFAULT_CORE = Path(__file__).resolve().parents[2] / "bitcoin_ipc_chain"
_SCHEMAS: dict[Path, dict] = {}

def require_capnp():
    try:
        import capnp
    except ImportError as exc:
        raise IPCError("Install dependencies: python -m pip install -r requirements.txt") from exc
    return capnp

def load_schemas(core_source: Path) -> dict:
    """One schema identity per process; consistent import roots prevent ID clashes."""
    source = core_source.resolve()
    if _SCHEMAS and source not in _SCHEMAS:
        raise IPCError("Use a fresh process to load a different Core schema checkout")
    if source in _SCHEMAS:
        return _SCHEMAS[source]
    capnp = require_capnp()
    src = source / "src"
    mp = src / "ipc/libmultiprocess/include"
    binary = shutil.which("capnp")
    includes = [str(mp), str(src), str(Path(capnp.__file__).resolve().parent.parent)]
    if binary:
        includes.insert(1, str(Path(binary).resolve().parent.parent / "include"))
    paths = {"proxy": mp / "mp/proxy.capnp"}
    paths.update({name: src / f"ipc/capnp/{name}.capnp" for name in ("init", "echo", "rpc", "chain")})
    missing = [str(path) for path in paths.values() if not path.is_file()]
    if missing:
        raise UnsupportedInterface("Required experimental schemas are missing: " + ", ".join(missing))
    modules = {name: capnp.load(str(path), imports=includes) for name, path in paths.items()}
    _SCHEMAS[source] = modules
    return modules

def provenance(core_source: Path) -> dict:
    source = core_source.resolve()
    result: dict[str, Any] = {"core_source": str(source), "schema_sha256": {}}
    commit = subprocess.run(["git", "-C", str(source), "rev-parse", "HEAD"], capture_output=True, text=True)
    result["core_commit"] = commit.stdout.strip() if commit.returncode == 0 else None
    status = subprocess.run(["git", "-C", str(source), "status", "--porcelain"], capture_output=True, text=True)
    result["core_dirty"] = bool(status.stdout.strip()) if status.returncode == 0 else None
    for root in (source / "src/ipc/capnp", source / "src/ipc/libmultiprocess/include/mp"):
        for path in sorted(root.glob("*.capnp")):
            result["schema_sha256"][str(path.relative_to(source))] = hashlib.sha256(path.read_bytes()).hexdigest()
    return result
