#!/usr/bin/env python3
"""Command-line entrypoint. Read cli.py for dispatch and bitcoin_ipc_client for IPC."""
from bitcoin_ipc_client import BitcoinIPCClient

__all__ = ["BitcoinIPCClient"]


if __name__ == "__main__":
    from cli import main

    raise SystemExit(main())
