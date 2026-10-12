"""Resolve Core-style Unix socket addresses without creating directories."""

from __future__ import annotations

import os
from pathlib import Path
import sys

CHAIN_DIRS = {
    "main": "",
    "test": "testnet3",
    "testnet4": "testnet4",
    "signet": "signet",
    "regtest": "regtest",
}


def default_data_dir() -> Path:
    if sys.platform == "win32":
        roaming = os.environ.get("APPDATA")
        if roaming and (legacy := Path(roaming) / "Bitcoin").is_dir():
            return legacy
        local = os.environ.get("LOCALAPPDATA")
        if not local:
            raise ValueError("Cannot locate Core's default data directory; specify --datadir")
        return Path(local) / "Bitcoin"
    home = Path(os.environ.get("HOME") or "/")
    if sys.platform == "darwin":
        return home / "Library/Application Support/Bitcoin"
    return home / ".bitcoin"


def resolve_socket(address: str | Path, *, chain: str = "main", datadir: str | Path | None = None) -> str:
    if chain not in CHAIN_DIRS:
        raise ValueError(f"Unknown chain: {chain}")
    value = str(address)
    if value == "unix" or value.startswith("unix:"):
        path = Path(value[5:]) if value.startswith("unix:") and value[5:] else Path("node.sock")
        if not path.is_absolute():
            base = Path(datadir).expanduser() if datadir is not None else default_data_dir()
            path = base / CHAIN_DIRS[chain] / path
    else:
        path = Path(value).expanduser()
    return str(path.resolve())
